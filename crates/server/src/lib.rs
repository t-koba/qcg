mod server;

pub use server::*;

#[cfg(test)]
pub(crate) fn test_service(
    generators_dir: camino::Utf8PathBuf,
    runs_dir: camino::Utf8PathBuf,
    providers_path: Option<camino::Utf8PathBuf>,
) -> Result<service::LocalService, service::ServiceError> {
    // E04: server tests build through the policy constructor with explicit
    // defaults.
    service::LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        providers_path,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        service::RunStoreMode::Exclusive,
        service::ServiceDeploymentPolicy::default(),
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
    service: service::LocalService,
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
        idempotency_ttl: policy::IDEMPOTENCY_TTL,
        idempotency_max_entries: policy::IDEMPOTENCY_MAX_ENTRIES,
        api_token_digest,
        artifact_limits: service::ArtifactZipLimits::default(),
        asset_limit: None,
        max_request_bytes: None,
        metrics_policy: crate::server::MetricsPolicy::default(),
        shutdown,
    }
}

#[cfg(test)]
mod tests;
