mod server;

pub use server::*;

#[cfg(test)]
pub(crate) fn test_service(
    generators_dir: camino::Utf8PathBuf,
    runs_dir: camino::Utf8PathBuf,
    providers_path: Option<camino::Utf8PathBuf>,
) -> Result<qcg_service::LocalQcgService, qcg_service::ServiceError> {
    // E04: server tests build through the policy constructor with explicit
    // defaults.
    qcg_service::LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        providers_path,
        qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
        qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
        qcg_service::RunStoreMode::Exclusive,
        qcg_service::ServiceDeploymentPolicy::default(),
    )
}

/// `AppState` for a test that does not exercise OAuth, bearer auth, artifact
/// or asset limits, or request body limits: no OAuth origin, callback, or
/// allowed origin, an empty idempotency map, the frozen default idempotency
/// policy, default artifact zip limits, and no asset or body limit. A test
/// that needs something else overrides only that field.
///
/// `api_token_digest` and `shutdown` stay explicit arguments instead of
/// defaults because tests genuinely differ on both: an auth test passes a
/// digest, and a drain test passes the token it cancels so the same token
/// drives the gate and the state.
#[cfg(test)]
pub(crate) fn test_state(
    service: qcg_service::LocalQcgService,
    runs_dir: camino::Utf8PathBuf,
    api_token_digest: Option<[u8; 32]>,
    shutdown: tokio_util::sync::CancellationToken,
) -> AppState {
    AppState {
        service,
        runs_dir,
        oauth_origin: None,
        oauth_allowed_origins: std::collections::BTreeSet::new(),
        oauth_callback_url: None,
        idempotency: tokio::sync::Mutex::new(std::collections::BTreeMap::new()),
        idempotency_ttl: qcg_policy::IDEMPOTENCY_TTL,
        idempotency_max_entries: qcg_policy::IDEMPOTENCY_MAX_ENTRIES,
        api_token_digest,
        artifact_limits: qcg_service::ArtifactZipLimits::default(),
        asset_limit: None,
        max_request_bytes: None,
        shutdown,
    }
}

#[cfg(test)]
mod tests;
