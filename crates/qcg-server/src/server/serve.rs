use anyhow::Result;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, Method, header};
use axum::middleware as axum_middleware;
use axum::routing::{get, put};
use qcg_service::LocalQcgService;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use super::config::{AppState, ServerConfig};
use super::generators::{
    describe_generator, healthz, list_generators, openapi, read_generator_asset,
};
use super::mcp::{
    cancel_pending_mcp_authorization, clear_mcp_authorization, complete_mcp_authorization,
    list_mcp_servers, start_mcp_authorization,
};
use super::middleware::{
    loopback_oauth_origins, metrics, reject_unsafe_generator_asset_path, require_api_auth,
    security_headers_middleware, sha256_bytes,
};
use super::run_detail::{
    answer_run, cancel_run_from_path, confirm_run, delete_run, dispatch_fallback, read_artifact,
    read_artifacts_zip, read_cost_metrics, read_journal, read_run_bundle, run_artifacts,
    run_events, run_snapshot,
};
use super::runs::{fork_run, list_runs, start_run};
use qcg_policy::IDEMPOTENCY_HEADER;

pub async fn serve_with_listener(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
) -> Result<SocketAddr> {
    let actual_addr = listener.local_addr()?;
    let mut roots = vec![config.generators_dir.clone()];
    roots.extend(config.extra_generators_dirs.clone());
    let service = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
        roots,
        config.runs_dir.clone(),
        config.providers_path.clone(),
        config.max_active_runs,
        config.max_tracked_runs,
        config.run_store_mode,
    )?;
    service.resume_recovered_runs().await;
    let _shared_recovery_task = service.start_shared_store_recovery();
    let _queued_resumer_task = service.start_queued_resumer();
    let _gc_task = service.start_retention_gc();
    let oauth_origin = actual_addr
        .ip()
        .is_loopback()
        .then(|| format!("http://{actual_addr}"));
    let oauth_allowed_origins = loopback_oauth_origins(actual_addr);
    let oauth_callback_url = oauth_origin
        .as_ref()
        .map(|origin| format!("{origin}/api/mcp/oauth/callback"));
    let state = Arc::new(AppState {
        service,
        oauth_origin,
        oauth_allowed_origins,
        oauth_callback_url,
        idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
        api_token_digest: config.api_token.as_deref().map(sha256_bytes),
        artifact_limits: qcg_service::ArtifactZipLimits {
            max_bytes: config.max_artifact_bytes,
            max_entries: config.max_artifact_entries,
        },
        asset_limit: config.max_asset_bytes,
    });
    let shutdown_service = state.service.clone();
    let app = build_router(&state, &config)?;
    tracing::info!(%actual_addr, "qcg server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_service))
        .await?;
    Ok(actual_addr)
}

pub(crate) fn build_router(state: &Arc<AppState>, config: &ServerConfig) -> Result<Router> {
    let mut app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/api/openapi.json", get(openapi))
        .route("/api/generators", get(list_generators))
        .route("/api/generators/{id}", get(describe_generator))
        .route(
            "/api/generators/{id}/assets/{*path}",
            get(read_generator_asset),
        )
        .route("/api/mcp/servers", get(list_mcp_servers))
        .route(
            "/api/mcp/servers/{id}/authorization",
            axum::routing::post(start_mcp_authorization).delete(clear_mcp_authorization),
        )
        .route(
            "/api/mcp/servers/{id}/authorization/pending",
            axum::routing::delete(cancel_pending_mcp_authorization),
        )
        .route("/api/mcp/oauth/callback", get(complete_mcp_authorization))
        .route("/api/runs", get(list_runs).post(start_run))
        .route(
            "/api/runs/{id}",
            get(run_snapshot)
                .post(cancel_run_from_path)
                .delete(delete_run),
        )
        .route("/api/runs/{id}/fork", axum::routing::post(fork_run))
        .route("/api/runs/{id}/questions/{qid}", put(answer_run))
        .route("/api/runs/{id}/confirmations/{cid}", put(confirm_run))
        .route("/api/runs/{id}/events", get(run_events))
        .route("/api/runs/{id}/artifacts", get(run_artifacts))
        .route("/api/runs/{id}/artifacts.zip", get(read_artifacts_zip))
        .route("/api/runs/{id}/bundle", get(read_run_bundle))
        .route("/api/runs/{id}/artifacts/{*path}", get(read_artifact))
        .route("/api/runs/{id}/journal", get(read_journal))
        .route("/api/runs/{id}/metrics", get(read_cost_metrics))
        .fallback(dispatch_fallback)
        .layer(axum_middleware::from_fn(reject_unsafe_generator_asset_path))
        .layer(axum_middleware::from_fn(security_headers_middleware))
        .layer(axum_middleware::from_fn_with_state(
            Arc::clone(state),
            require_api_auth,
        ))
        .with_state(Arc::clone(state));
    if let Some(max_request_bytes) = config.max_request_bytes {
        app = app.layer(DefaultBodyLimit::max(max_request_bytes));
    }
    let app = if !config.cors_origins.is_empty() {
        let origins = config
            .cors_origins
            .iter()
            .map(|origin| origin.parse::<HeaderValue>())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        app.layer(
            CorsLayer::new()
                .allow_origin(tower_http::cors::AllowOrigin::list(origins))
                .allow_headers([
                    header::AUTHORIZATION,
                    header::CONTENT_TYPE,
                    header::HeaderName::from_static(IDEMPOTENCY_HEADER),
                ])
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::DELETE,
                    Method::OPTIONS,
                ]),
        )
    } else {
        app
    };
    Ok(app)
}

pub(crate) async fn shutdown_signal(service: LocalQcgService) {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::error!(%error, "failed to install SIGTERM handler");
                    if let Err(error) = tokio::signal::ctrl_c().await {
                        tracing::error!(%error, "failed to wait for Ctrl-C");
                        return;
                    }
                    if let Err(error) = service.shutdown_active_runs().await {
                        tracing::error!(%error, "failed to stop active runs");
                    }
                    return;
                }
            };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "failed to wait for Ctrl-C");
                    return;
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to wait for Ctrl-C");
        return;
    }
    if let Err(error) = service.shutdown_active_runs().await {
        tracing::error!(%error, "failed to stop active runs");
    }
    std::process::exit(0);
}
