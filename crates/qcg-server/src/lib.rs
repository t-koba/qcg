use anyhow::Result;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, OriginalUri, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use camino::Utf8PathBuf;
use futures_util::StreamExt as FuturesStreamExt;
use qcg_api::{
    AnswerPayload, ApiError, ConfirmDecision, ForkRun, GeneratorSummary, McpAuthorizationStart,
    McpServerList, ProblemDetails, ProblemFieldError, RunListQuery, RunListResponse, RunSnapshot,
    StartRun,
};
use qcg_service::{LocalQcgService, RunStoreMode};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use tower_http::cors::CorsLayer;

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const IDEMPOTENCY_MAX_ENTRIES: usize = 1024;
const RUN_REQUEST_BODY_LIMIT: usize = qcg_types::MAX_FILE_INPUT_BYTES * 2;

#[cfg(test)]
const SERVER_ROUTES: &[(&str, &str, Option<&str>, Option<qcg_api::ResponseSchema>)] = &[
    ("get", "/healthz", None, None),
    ("get", "/metrics", None, None),
    ("get", "/api/openapi.json", None, None),
    (
        "get",
        "/api/generators",
        None,
        Some(qcg_api::ResponseSchema::ArrayRef("GeneratorSummary")),
    ),
    (
        "get",
        "/api/generators/{id}",
        None,
        Some(qcg_api::ResponseSchema::Ref("GeneratorDetail")),
    ),
    ("get", "/api/generators/{id}/assets/{path}", None, None),
    (
        "get",
        "/api/mcp/servers",
        None,
        Some(qcg_api::ResponseSchema::Ref("McpServerList")),
    ),
    (
        "post",
        "/api/mcp/servers/{id}/authorization",
        None,
        Some(qcg_api::ResponseSchema::Ref("McpAuthorizationStart")),
    ),
    ("delete", "/api/mcp/servers/{id}/authorization", None, None),
    (
        "delete",
        "/api/mcp/servers/{id}/authorization/pending",
        None,
        None,
    ),
    ("get", "/api/mcp/oauth/callback", None, None),
    (
        "get",
        "/api/runs",
        None,
        Some(qcg_api::ResponseSchema::Ref("RunListResponse")),
    ),
    (
        "post",
        "/api/runs",
        Some("StartRun"),
        Some(qcg_api::ResponseSchema::Ref("RunSnapshot")),
    ),
    (
        "get",
        "/api/runs/{id}",
        None,
        Some(qcg_api::ResponseSchema::Ref("RunSnapshot")),
    ),
    (
        "post",
        "/api/runs/{id}/fork",
        Some("ForkRun"),
        Some(qcg_api::ResponseSchema::Ref("RunSnapshot")),
    ),
    (
        "put",
        "/api/runs/{id}/questions/{qid}",
        Some("AnswerPayload"),
        Some(qcg_api::ResponseSchema::Ref("RunSnapshot")),
    ),
    (
        "put",
        "/api/runs/{id}/confirmations/{cid}",
        Some("ConfirmDecision"),
        Some(qcg_api::ResponseSchema::Ref("RunSnapshot")),
    ),
    (
        "post",
        "/api/runs/{id}:cancel",
        None,
        Some(qcg_api::ResponseSchema::Ref("RunSnapshot")),
    ),
    ("get", "/api/runs/{id}/events", None, None),
    (
        "get",
        "/api/runs/{id}/artifacts",
        None,
        Some(qcg_api::ResponseSchema::Ref("OutputManifest")),
    ),
    ("get", "/api/runs/{id}/artifacts/{path}", None, None),
    ("get", "/api/runs/{id}/artifacts.zip", None, None),
    ("get", "/api/runs/{id}/journal", None, None),
];

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
}

#[derive(Debug)]
struct AppState {
    service: LocalQcgService,
    oauth_origin: Option<String>,
    oauth_allowed_origins: BTreeSet<String>,
    oauth_callback_url: Option<String>,
    idempotency: tokio::sync::Mutex<BTreeMap<String, IdempotencyEntry>>,
    api_token_digest: Option<[u8; 32]>,
}

#[derive(Debug)]
enum IdempotencyEntry {
    Pending {
        digest: String,
        owner_id: uuid::Uuid,
        created_at: Instant,
        completed: tokio::sync::watch::Sender<bool>,
    },
    Ready {
        digest: String,
        created_at: Instant,
        run_id: String,
    },
}

struct PendingIdempotencyGuard {
    state: Arc<AppState>,
    key: String,
    owner_id: uuid::Uuid,
    armed: bool,
}

impl PendingIdempotencyGuard {
    fn new(state: Arc<AppState>, key: String, owner_id: uuid::Uuid) -> Self {
        Self {
            state,
            key,
            owner_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingIdempotencyGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let state = Arc::clone(&self.state);
        let key = self.key.clone();
        let owner_id = self.owner_id;
        tokio::spawn(async move {
            let completed = {
                let mut entries = state.idempotency.lock().await;
                if matches!(
                    entries.get(&key),
                    Some(IdempotencyEntry::Pending {
                        owner_id: current,
                        ..
                    }) if *current == owner_id
                ) {
                    match entries.remove(&key) {
                        Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                        _ => None,
                    }
                } else {
                    None
                }
            };
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
        });
    }
}

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
    });
    let shutdown_service = state.service.clone();
    let app = Router::new()
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
            get(run_snapshot).post(cancel_run_from_path),
        )
        .route("/api/runs/{id}/fork", axum::routing::post(fork_run))
        .route("/api/runs/{id}/questions/{qid}", put(answer_run))
        .route("/api/runs/{id}/confirmations/{cid}", put(confirm_run))
        .route("/api/runs/{id}/events", get(run_events))
        .route("/api/runs/{id}/artifacts", get(run_artifacts))
        .route("/api/runs/{id}/artifacts.zip", get(read_artifacts_zip))
        .route("/api/runs/{id}/artifacts/{*path}", get(read_artifact))
        .route("/api/runs/{id}/journal", get(read_journal))
        .fallback(dispatch_fallback)
        .layer(DefaultBodyLimit::max(RUN_REQUEST_BODY_LIMIT))
        .layer(middleware::from_fn(reject_unsafe_generator_asset_path))
        .layer(middleware::from_fn(security_headers_middleware))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            require_api_auth,
        ))
        .with_state(state);
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

    tracing::info!(%actual_addr, "qcg server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_service))
        .await?;
    Ok(actual_addr)
}

fn sha256_bytes(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

async fn require_api_auth(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if state.api_token_digest.is_none()
        || matches!(
            path,
            "/healthz" | "/api/openapi.json" | "/api/mcp/oauth/callback"
        )
    {
        return next.run(request).await;
    }
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(sha256_bytes);
    let authorized = supplied.is_some_and(|supplied| {
        constant_time_digest_eq(
            &supplied,
            state
                .api_token_digest
                .as_ref()
                .expect("checked token digest"),
        )
    });
    if authorized {
        next.run(request).await
    } else {
        let mut response =
            ApiHttpError::unauthorized("valid bearer token required").into_response();
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"qcg\""),
        );
        response
    }
}

async fn metrics(State(state): State<Arc<AppState>>) -> Result<Response, ApiHttpError> {
    let runs = state
        .service
        .list_run_items()
        .await
        .map_err(ApiHttpError::from_api)?;
    let mut states = BTreeMap::<String, usize>::new();
    for run in &runs {
        *states.entry(run.state.to_string()).or_default() += 1;
    }
    let active = runs
        .iter()
        .filter(|run| {
            matches!(
                run.state,
                qcg_api::RunStatus::Queued | qcg_api::RunStatus::Running
            )
        })
        .count();
    let mut body = String::from(
        "# HELP qcg_runs_total Number of durable runs by state.\n# TYPE qcg_runs_total gauge\n",
    );
    for (status, count) in states {
        body.push_str(&format!("qcg_runs_total{{state=\"{status}\"}} {count}\n"));
    }
    body.push_str("# HELP qcg_runs_active Number of queued or running runs.\n");
    body.push_str("# TYPE qcg_runs_active gauge\n");
    body.push_str(&format!("qcg_runs_active {active}\n"));
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

fn constant_time_digest_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn loopback_oauth_origins(address: SocketAddr) -> BTreeSet<String> {
    if !address.ip().is_loopback() {
        return BTreeSet::new();
    }
    BTreeSet::from([
        format!("http://{address}"),
        format!("http://localhost:{}", address.port()),
        format!("http://127.0.0.1:{}", address.port()),
        format!("http://[::1]:{}", address.port()),
    ])
}

async fn shutdown_signal(service: LocalQcgService) {
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

async fn reject_unsafe_generator_asset_path(
    request: Request,
    next: Next,
) -> Result<Response, ApiHttpError> {
    if request.method() == Method::GET && unsafe_generator_asset_path(request.uri().path()) {
        return Err(ApiHttpError::bad_request(
            "generator asset path contains an unsafe path component",
        ));
    }
    Ok(next.run(request).await)
}

async fn security_headers_middleware(request: Request, next: Next) -> Response {
    let generator_asset = request.uri().path().starts_with("/api/generators/")
        && request.uri().path().contains("/assets/");
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            if generator_asset {
                "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src 'self' data: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
            } else {
                "default-src 'none'; frame-ancestors 'none'"
            },
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn openapi() -> Json<Value> {
    Json(qcg_api::openapi_document(env!("CARGO_PKG_VERSION")))
}

async fn list_generators(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<GeneratorSummary>>, ApiHttpError> {
    state
        .service
        .list_generators()
        .await
        .map(Json)
        .map_err(ApiHttpError::from_api)
}

async fn describe_generator(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let detail = state
        .service
        .describe(&id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&detail).map_err(ApiHttpError::internal)?)
    );
    conditional_json(&headers, weak_etag(&format!("generator-{digest}")), &detail)
}

async fn read_generator_asset(
    State(state): State<Arc<AppState>>,
    Path((id, path)): Path<(String, String)>,
) -> Result<Response, ApiHttpError> {
    let bytes = state
        .service
        .read_generator_asset(id, path.clone())
        .await
        .map_err(ApiHttpError::from_api)?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type_for_name(&path))
        .header(header::CONTENT_DISPOSITION, "inline")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(bytes))
        .map_err(ApiHttpError::internal)
}

async fn list_mcp_servers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<McpServerList>, ApiHttpError> {
    state
        .service
        .list_mcp_servers()
        .await
        .map(Json)
        .map_err(ApiHttpError::internal)
}

async fn start_mcp_authorization(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<McpAuthorizationStart>, ApiHttpError> {
    require_local_oauth_origin(&state, &headers)?;
    let callback_url = state.oauth_callback_url.as_deref().ok_or_else(|| {
        ApiHttpError::forbidden("MCP OAuth is available only on a loopback listener")
    })?;
    state
        .service
        .start_mcp_authorization(&id, callback_url)
        .await
        .map(Json)
        .map_err(|error| ApiHttpError::bad_request(error.to_string()))
}

async fn clear_mcp_authorization(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiHttpError> {
    require_local_oauth_origin(&state, &headers)?;
    state
        .service
        .clear_mcp_authorization(&id)
        .await
        .map_err(|error| ApiHttpError::bad_request(error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn cancel_pending_mcp_authorization(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiHttpError> {
    require_local_oauth_origin(&state, &headers)?;
    state
        .service
        .cancel_pending_mcp_authorization(&id)
        .await
        .map_err(|error| ApiHttpError::bad_request(error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn complete_mcp_authorization(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, ApiHttpError> {
    let origin = state.oauth_origin.as_deref().ok_or_else(|| {
        ApiHttpError::forbidden("MCP OAuth is available only on a loopback listener")
    })?;
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(uri.path());
    let callback_url = format!("{origin}{path_and_query}");
    let server_id = state
        .service
        .complete_mcp_authorization(&callback_url)
        .await
        .map_err(|error| ApiHttpError::bad_request(error.to_string()))?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(format!(
            "<!doctype html><meta charset=\"utf-8\"><title>MCP connected</title><p>MCP server <code>{server_id}</code> is connected. You can close this window.</p>"
        )))
        .map_err(ApiHttpError::internal)
}

fn require_local_oauth_origin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiHttpError> {
    if state.oauth_origin.is_none() {
        return Err(ApiHttpError::forbidden(
            "MCP OAuth is available only on a loopback listener",
        ));
    }
    if state.oauth_allowed_origins.is_empty() {
        return Err(ApiHttpError::forbidden(
            "MCP OAuth has no allowed loopback origins",
        ));
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin
            .to_str()
            .map_err(|_| ApiHttpError::forbidden("request Origin is invalid"))?;
        if !state.oauth_allowed_origins.contains(origin) {
            return Err(ApiHttpError::forbidden(
                "cross-origin MCP authorization is not allowed",
            ));
        }
    }
    Ok(())
}

async fn list_runs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RunListQuery>,
) -> Result<Json<RunListResponse>, ApiHttpError> {
    let limit = query.limit.unwrap_or(50);
    if limit == 0 || limit > 200 {
        return Err(ApiHttpError::bad_request_field(
            "limit",
            "limit must be between 1 and 200",
        ));
    }
    let since = query
        .since
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|error| {
            ApiHttpError::bad_request_field("since", format!("since must be RFC 3339: {error}"))
        })?;
    let mut items = state
        .service
        .list_run_items()
        .await
        .map_err(ApiHttpError::from_api)?;
    items.retain(|item| {
        query.state.is_none_or(|run_state| item.state == run_state)
            && query
                .generator_id
                .as_ref()
                .is_none_or(|generator_id| &item.generator_id == generator_id)
            && since.is_none_or(|since| {
                chrono::DateTime::parse_from_rfc3339(&item.started_at)
                    .is_ok_and(|started_at| started_at >= since)
            })
            && query
                .cursor
                .as_ref()
                .is_none_or(|cursor| &item.run_id > cursor)
    });
    items.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    let next_cursor = (items.len() > limit).then(|| items[limit - 1].run_id.clone());
    items.truncate(limit);
    Ok(Json(RunListResponse { items, next_cursor }))
}

async fn start_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<StartRun>,
) -> Result<Response, ApiHttpError> {
    let request_digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&req).map_err(ApiHttpError::internal)?)
    );
    let idempotency_key = headers
        .get(IDEMPOTENCY_HEADER)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Idempotency-Key must be valid ASCII"))?
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string);
    let Some(idempotency_key) = idempotency_key else {
        return start_new_run(&state, req).await;
    };
    let owner_id = loop {
        let (wait, owner_id) = {
            let mut idempotency = state.idempotency.lock().await;
            prune_idempotency(&mut idempotency, Instant::now());
            match idempotency.get(&idempotency_key) {
                Some(IdempotencyEntry::Ready { digest, run_id, .. }) => {
                    if digest != &request_digest {
                        return Err(idempotency_conflict());
                    }
                    let run_id = run_id.clone();
                    drop(idempotency);
                    let snapshot = state
                        .service
                        .snapshot(run_id)
                        .await
                        .map_err(ApiHttpError::from_api)?;
                    return created_run_response(snapshot);
                }
                Some(IdempotencyEntry::Pending {
                    digest, completed, ..
                }) => {
                    if digest != &request_digest {
                        return Err(idempotency_conflict());
                    }
                    (Some(completed.subscribe()), None)
                }
                None => {
                    if idempotency.len() >= IDEMPOTENCY_MAX_ENTRIES {
                        return Err(ApiHttpError::service_unavailable(
                            "too many idempotent requests are still in progress",
                        ));
                    }
                    let (completed, _) = tokio::sync::watch::channel(false);
                    let owner_id = uuid::Uuid::now_v7();
                    idempotency.insert(
                        idempotency_key.clone(),
                        IdempotencyEntry::Pending {
                            digest: request_digest.clone(),
                            owner_id,
                            created_at: Instant::now(),
                            completed,
                        },
                    );
                    (None, Some(owner_id))
                }
            }
        };
        if let Some(owner_id) = owner_id {
            break owner_id;
        }
        if let Some(mut completed) = wait {
            if !*completed.borrow() {
                let _ = completed.changed().await;
            }
            continue;
        }
        unreachable!("idempotency admission must wait or assign an owner");
    };
    let mut pending_guard =
        PendingIdempotencyGuard::new(Arc::clone(&state), idempotency_key.clone(), owner_id);
    let run_id = match state.service.start_run(req).await {
        Ok(run_id) => run_id,
        Err(error) => {
            let completed = {
                let mut idempotency = state.idempotency.lock().await;
                if matches!(
                    idempotency.get(&idempotency_key),
                    Some(IdempotencyEntry::Pending {
                        owner_id: current,
                        ..
                    }) if *current == owner_id
                ) {
                    match idempotency.remove(&idempotency_key) {
                        Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                        _ => None,
                    }
                } else {
                    None
                }
            };
            pending_guard.disarm();
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
            return Err(ApiHttpError::from_api(error));
        }
    };
    let completed = {
        let mut idempotency = state.idempotency.lock().await;
        let completed = if matches!(
            idempotency.get(&idempotency_key),
            Some(IdempotencyEntry::Pending {
                owner_id: current,
                ..
            }) if *current == owner_id
        ) {
            match idempotency.remove(&idempotency_key) {
                Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                _ => None,
            }
        } else {
            None
        };
        idempotency.insert(
            idempotency_key,
            IdempotencyEntry::Ready {
                digest: request_digest,
                created_at: Instant::now(),
                run_id: run_id.clone(),
            },
        );
        completed
    };
    pending_guard.disarm();
    if let Some(completed) = completed {
        let _ = completed.send(true);
    }
    let snapshot = state
        .service
        .snapshot(run_id.clone())
        .await
        .map_err(ApiHttpError::from_api)?;
    created_run_response(snapshot)
}

async fn start_new_run(state: &AppState, req: StartRun) -> Result<Response, ApiHttpError> {
    let run_id = state
        .service
        .start_run(req)
        .await
        .map_err(ApiHttpError::from_api)?;
    let snapshot = state
        .service
        .snapshot(run_id)
        .await
        .map_err(ApiHttpError::from_api)?;
    created_run_response(snapshot)
}

async fn fork_run(
    State(state): State<Arc<AppState>>,
    Path(source_id): Path<String>,
    Json(request): Json<ForkRun>,
) -> Result<Response, ApiHttpError> {
    let run_id = state
        .service
        .fork_run(&source_id, request)
        .await
        .map_err(ApiHttpError::from_api)?;
    let snapshot = state
        .service
        .snapshot(run_id)
        .await
        .map_err(ApiHttpError::from_api)?;
    created_run_response(snapshot)
}

fn idempotency_conflict() -> ApiHttpError {
    ApiHttpError::from_api(ApiError::Conflict {
        detail: "Idempotency-Key was already used with a different request".into(),
    })
}

fn prune_idempotency(entries: &mut BTreeMap<String, IdempotencyEntry>, now: Instant) {
    entries.retain(|_, entry| {
        let created_at = match entry {
            IdempotencyEntry::Pending { created_at, .. }
            | IdempotencyEntry::Ready { created_at, .. } => created_at,
        };
        now.duration_since(*created_at) < IDEMPOTENCY_TTL
    });
    while entries.len() >= IDEMPOTENCY_MAX_ENTRIES {
        let Some(oldest) = entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                IdempotencyEntry::Ready { created_at, .. } => Some((key, *created_at)),
                IdempotencyEntry::Pending { .. } => None,
            })
            .min_by_key(|(_, created_at)| *created_at)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        entries.remove(&oldest);
    }
}

fn created_run_response(snapshot: RunSnapshot) -> Result<Response, ApiHttpError> {
    let location = format!("/api/runs/{}", snapshot.run_id);
    Response::builder()
        .status(StatusCode::CREATED)
        .header(header::LOCATION, location)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&snapshot).map_err(ApiHttpError::internal)?,
        ))
        .map_err(ApiHttpError::internal)
}

async fn run_snapshot(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let snapshot = state
        .service
        .snapshot(id)
        .await
        .map_err(ApiHttpError::from_api)?;
    conditional_json(
        &headers,
        weak_etag(&format!("run-{}-{}", snapshot.seq, snapshot.state)),
        &snapshot,
    )
}

async fn answer_run(
    State(state): State<Arc<AppState>>,
    Path((id, question_id)): Path<(String, String)>,
    Json(payload): Json<AnswerPayload>,
) -> Result<Json<RunSnapshot>, ApiHttpError> {
    state
        .service
        .answer(id.clone(), question_id, payload)
        .await
        .map_err(ApiHttpError::from_api)?;
    state
        .service
        .snapshot(id)
        .await
        .map(Json)
        .map_err(ApiHttpError::from_api)
}

async fn confirm_run(
    State(state): State<Arc<AppState>>,
    Path((id, confirmation_id)): Path<(String, String)>,
    Json(decision): Json<ConfirmDecision>,
) -> Result<Json<RunSnapshot>, ApiHttpError> {
    state
        .service
        .confirm(id.clone(), confirmation_id, decision)
        .await
        .map_err(ApiHttpError::from_api)?;
    state
        .service
        .snapshot(id)
        .await
        .map(Json)
        .map_err(ApiHttpError::from_api)
}

async fn cancel_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<RunSnapshot>, ApiHttpError> {
    state
        .service
        .cancel(id.clone())
        .await
        .map_err(ApiHttpError::from_api)?;
    state
        .service
        .snapshot(id)
        .await
        .map(Json)
        .map_err(ApiHttpError::from_api)
}

async fn cancel_run_from_path(
    State(state): State<Arc<AppState>>,
    Path(path_id): Path<String>,
) -> Result<Response, ApiHttpError> {
    let Some(id) = path_id.strip_suffix(":cancel").filter(|id| !id.is_empty()) else {
        return Err(ApiHttpError::new(
            StatusCode::NOT_FOUND,
            "Resource not found",
            "not_found",
            format!("route `POST /api/runs/{path_id}` was not found"),
            format!("/api/runs/{path_id}"),
            Vec::new(),
        ));
    };
    cancel_run(State(state), Path(id.to_string()))
        .await
        .map(IntoResponse::into_response)
}

async fn dispatch_fallback(
    method: Method,
    original_uri: OriginalUri,
) -> Result<Response, ApiHttpError> {
    Err(ApiHttpError::new(
        StatusCode::NOT_FOUND,
        "Resource not found",
        "not_found",
        format!("route `{method} {}` was not found", original_uri.path()),
        original_uri.path(),
        Vec::new(),
    ))
}

fn unsafe_generator_asset_path(path: &str) -> bool {
    let Some((prefix, asset_path)) = path.split_once("/assets/") else {
        return false;
    };
    if !prefix.starts_with("/api/generators/") {
        return false;
    }
    let encoded = asset_path.to_ascii_lowercase();
    !qcg_types::is_safe_relative_path(asset_path)
        || encoded.contains("%2e")
        || encoded.contains("%2f")
        || encoded.contains("%5c")
}

async fn run_events(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiHttpError> {
    let after_seq = headers
        .get("last-event-id")
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Last-Event-ID must be valid ASCII"))?
        .map(str::parse::<u64>)
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Last-Event-ID must be an unsigned integer"))?
        .unwrap_or(0);
    let stream = state
        .service
        .subscribe(id)
        .await
        .map_err(ApiHttpError::from_api)?
        .filter_map(move |event| async move {
            if event.seq <= after_seq {
                return None;
            }
            let data = serde_json::to_string(&event).ok()?;
            let event = Event::default().id(event.seq.to_string()).data(data);
            Some(Ok(event))
        });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn run_artifacts(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let artifacts = state
        .service
        .artifacts(id.clone())
        .await
        .map_err(ApiHttpError::from_api)?;
    let snapshot = state
        .service
        .snapshot(id)
        .await
        .map_err(ApiHttpError::from_api)?;
    conditional_json(
        &headers,
        weak_etag(&format!("artifacts-{}", snapshot.seq)),
        &artifacts,
    )
}

fn weak_etag(value: &str) -> String {
    format!("W/\"{value}\"")
}

fn conditional_json<T: Serialize>(
    headers: &HeaderMap,
    etag: String,
    value: &T,
) -> Result<Response, ApiHttpError> {
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some(etag.as_str())
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, etag)
            .body(Body::empty())
            .map_err(ApiHttpError::internal);
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ETAG, etag)
        .body(Body::from(
            serde_json::to_vec(value).map_err(ApiHttpError::internal)?,
        ))
        .map_err(ApiHttpError::internal)
}

async fn read_artifact(
    State(state): State<Arc<AppState>>,
    Path((id, path)): Path<(String, String)>,
) -> Result<Response, ApiHttpError> {
    let content_disposition = content_disposition_attachment(&path);
    let (artifact, resolved) = state
        .service
        .read_artifact(id, path)
        .await
        .map_err(ApiHttpError::from_api)?;
    let content_type = artifact
        .mime
        .unwrap_or_else(|| content_type_for_name(&artifact.path).to_string());
    let file = tokio::fs::File::open(resolved)
        .await
        .map_err(ApiHttpError::internal)?;
    let metadata = file.metadata().await.map_err(ApiHttpError::internal)?;
    if !metadata.is_file() || metadata.len() != artifact.bytes {
        return Err(ApiHttpError::internal(format!(
            "artifact `{}` bytes mismatch: manifest={}, actual={}",
            artifact.path,
            artifact.bytes,
            metadata.len()
        )));
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, content_disposition)
        .header(header::CONTENT_LENGTH, artifact.bytes)
        .body(Body::from_stream(ReaderStream::new(file)))
        .map_err(ApiHttpError::internal)
}

async fn read_artifacts_zip(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiHttpError> {
    let run_dir = state
        .service
        .run_dir_for(&id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let (sender, receiver) = mpsc::channel::<Result<Vec<u8>, io::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter {
            sender: sender.clone(),
        };
        if let Err(error) = qcg_service::write_artifacts_zip_stream(&run_dir, writer) {
            let _ = sender.blocking_send(Err(io::Error::other(error)));
        }
    });
    let stream = ReceiverStream::new(receiver);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{id}-artifacts.zip\""),
        )
        .body(Body::from_stream(stream))
        .map_err(ApiHttpError::internal)
}

struct ChannelWriter {
    sender: mpsc::Sender<Result<Vec<u8>, io::Error>>,
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.sender
            .blocking_send(Ok(buf.to_vec()))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "zip response stream closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn read_journal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiHttpError> {
    let bytes = state
        .service
        .read_journal(id)
        .await
        .map_err(ApiHttpError::from_api)?
        .into_bytes();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(bytes))
        .map_err(ApiHttpError::internal)
}

fn content_type_for_name(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("gif") => "image/gif",
        Some("ico") => "image/x-icon",
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("woff2") => "font/woff2",
        Some("csv") => "text/csv; charset=utf-8",
        Some("xml") => "application/xml; charset=utf-8",
        Some("yaml" | "yml") => "application/yaml; charset=utf-8",
        Some("toml") => "application/toml; charset=utf-8",
        Some("pdf") => "application/pdf",
        Some("zip") => "application/zip",
        Some("md") => "text/markdown; charset=utf-8",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn content_disposition_attachment(path: &str) -> String {
    let file_name = path
        .rsplit('/')
        .next()
        .unwrap_or("artifact")
        .replace(['"', '\\', '\r', '\n'], "_");
    format!("attachment; filename=\"{file_name}\"")
}

#[derive(Debug)]
struct ApiHttpError {
    problem: Box<ProblemDetails>,
}

impl ApiHttpError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "Invalid request",
            "invalid_request",
            message,
            "",
            Vec::new(),
        )
    }

    fn bad_request_field(field: impl Into<String>, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        Self::new(
            StatusCode::BAD_REQUEST,
            "Invalid request",
            "invalid_request",
            reason.clone(),
            "",
            vec![ProblemFieldError {
                field: field.into(),
                reason,
            }],
        )
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "internal_error",
            error.to_string(),
            "",
            Vec::new(),
        )
    }

    fn unauthorized(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "unauthorized",
            detail,
            "",
            Vec::new(),
        )
    }

    fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "Forbidden",
            "forbidden",
            detail,
            "",
            Vec::new(),
        )
    }

    fn service_unavailable(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service unavailable",
            "service_unavailable",
            detail,
            "",
            Vec::new(),
        )
    }

    fn from_api(error: ApiError) -> Self {
        match error {
            ApiError::NotFound { detail } => Self::new(
                StatusCode::NOT_FOUND,
                "Resource not found",
                "not_found",
                detail,
                "",
                Vec::new(),
            ),
            ApiError::Invalid { field, reason } => {
                let errors: Vec<ProblemFieldError> = field
                    .map(|field| ProblemFieldError {
                        field,
                        reason: reason.clone(),
                    })
                    .into_iter()
                    .collect();
                let status = if errors.is_empty() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::UNPROCESSABLE_ENTITY
                };
                Self::new(
                    status,
                    "Invalid request",
                    if status == StatusCode::UNPROCESSABLE_ENTITY {
                        "validation_failed"
                    } else {
                        "invalid_request"
                    },
                    reason,
                    "",
                    errors,
                )
            }
            ApiError::TooLarge {
                limit_bytes,
                actual_bytes,
            } => Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Payload too large",
                "payload_too_large",
                format!("payload is {actual_bytes} bytes; limit is {limit_bytes} bytes"),
                "",
                Vec::new(),
            ),
            ApiError::Conflict { detail } => Self::new(
                StatusCode::CONFLICT,
                "Resource conflict",
                "conflict",
                detail,
                "",
                Vec::new(),
            ),
            ApiError::Unsupported { detail } => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Unsupported operation",
                "unsupported",
                detail,
                "",
                Vec::new(),
            ),
            ApiError::Unavailable { detail } => Self::service_unavailable(detail),
            ApiError::Internal { detail } => Self::internal(detail),
        }
    }

    fn new(
        status: StatusCode,
        title: impl Into<String>,
        code: impl Into<String>,
        detail: impl Into<String>,
        instance: impl Into<String>,
        errors: Vec<ProblemFieldError>,
    ) -> Self {
        let code = code.into();
        Self {
            problem: Box::new(ProblemDetails {
                problem_type: format!("https://qcg.dev/problems/{code}"),
                title: title.into(),
                status: status.as_u16(),
                detail: detail.into(),
                instance: instance.into(),
                code,
                errors,
            }),
        }
    }
}

impl axum::response::IntoResponse for ApiHttpError {
    fn into_response(self) -> axum::response::Response {
        let status =
            StatusCode::from_u16(self.problem.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut response = (status, Json(self.problem)).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{StatusCode, header};
    use std::sync::Arc;

    #[test]
    fn server_routes_match_openapi_route_table() {
        let mut server = super::SERVER_ROUTES.to_vec();
        server.sort();
        let mut documented = qcg_api::API_ROUTES
            .iter()
            .map(|route| {
                (
                    route.method,
                    route.path,
                    route.request_schema,
                    match route.response.body {
                        qcg_api::ResponseBody::Json(schema) => schema,
                        qcg_api::ResponseBody::Binary(_)
                        | qcg_api::ResponseBody::Text(_)
                        | qcg_api::ResponseBody::Empty => None,
                    },
                )
            })
            .collect::<Vec<_>>();
        documented.sort();
        assert_eq!(server, documented);
    }

    #[test]
    fn oauth_accepts_only_loopback_aliases_on_the_bound_port() {
        let origins = loopback_oauth_origins("127.0.0.1:43123".parse().expect("valid address"));
        assert!(origins.contains("http://127.0.0.1:43123"));
        assert!(origins.contains("http://localhost:43123"));
        assert!(origins.contains("http://[::1]:43123"));
        assert!(!origins.contains("http://localhost:43124"));

        let remote = loopback_oauth_origins("192.0.2.1:43123".parse().expect("valid address"));
        assert!(remote.is_empty());
    }

    #[test]
    fn bearer_digest_comparison_rejects_any_difference() {
        let expected = sha256_bytes("correct-token");
        assert!(constant_time_digest_eq(
            &expected,
            &sha256_bytes("correct-token")
        ));
        assert!(!constant_time_digest_eq(
            &expected,
            &sha256_bytes("wrong-token")
        ));
    }

    #[tokio::test]
    async fn explicit_providers_path_is_authoritative() {
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-server-providers-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("listener should bind: {error}"),
        };
        let missing = root.join("providers.toml");
        let error = super::serve_with_listener(
            ServerConfig {
                generators_dir: root.join("generators"),
                providers_path: Some(missing.clone()),
                extra_generators_dirs: Vec::new(),
                runs_dir: root.join("runs"),
                max_active_runs: qcg_service::DEFAULT_MAX_ACTIVE_RUNS,
                max_tracked_runs: qcg_service::DEFAULT_MAX_TRACKED_RUNS,
                run_store_mode: RunStoreMode::Exclusive,
                cors_origins: Vec::new(),
                api_token: None,
            },
            listener,
        )
        .await
        .expect_err("an explicit missing path must not fall back to another registry");
        assert!(error.to_string().contains(missing.as_str()));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn concurrent_idempotent_start_creates_one_run() {
        let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-idempotency-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
        });
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("same-run"));
        let request = StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
        };

        let (first, second) = tokio::join!(
            start_run(
                State(Arc::clone(&state)),
                headers.clone(),
                Json(request.clone())
            ),
            start_run(State(Arc::clone(&state)), headers, Json(request))
        );
        let first = first.expect("first request should start");
        let second = second.expect("second request should reuse the run");
        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CREATED);
        assert_eq!(
            first.headers().get(header::LOCATION),
            second.headers().get(header::LOCATION)
        );
        assert_eq!(
            state
                .service
                .list_runs()
                .await
                .expect("runs should be listable")
                .len(),
            1
        );

        let invalid_since = list_runs(
            State(Arc::clone(&state)),
            Query(RunListQuery {
                since: Some("not-a-date".into()),
                ..RunListQuery::default()
            }),
        )
        .await
        .expect_err("invalid since must be rejected");
        assert_eq!(
            invalid_since.problem.status,
            StatusCode::BAD_REQUEST.as_u16()
        );
        assert_eq!(invalid_since.problem.errors[0].field, "since");

        {
            let mut entries = state.idempotency.lock().await;
            entries.clear();
            for index in 0..IDEMPOTENCY_MAX_ENTRIES {
                let (completed, _) = tokio::sync::watch::channel(false);
                entries.insert(
                    format!("pending-{index}"),
                    IdempotencyEntry::Pending {
                        digest: "digest".into(),
                        owner_id: uuid::Uuid::now_v7(),
                        created_at: Instant::now(),
                        completed,
                    },
                );
            }
        }
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("at-capacity"));
        let error = start_run(
            State(Arc::clone(&state)),
            headers,
            Json(StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
            }),
        )
        .await
        .expect_err("pending entries must not be evicted at capacity");
        assert_eq!(
            error.problem.status,
            StatusCode::SERVICE_UNAVAILABLE.as_u16()
        );
        assert!(
            state
                .idempotency
                .lock()
                .await
                .values()
                .all(|entry| matches!(entry, IdempotencyEntry::Pending { .. }))
        );
    }

    #[tokio::test]
    async fn cancelled_idempotency_owner_releases_pending_entry() {
        let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-idempotency-cancel-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(workspace.join("fixtures/generators"), runs, None)
                .expect("service should initialize"),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
        });
        let owner_id = uuid::Uuid::now_v7();
        let (completed, _) = tokio::sync::watch::channel(false);
        state.idempotency.lock().await.insert(
            "cancelled".into(),
            IdempotencyEntry::Pending {
                digest: "digest".into(),
                owner_id,
                created_at: Instant::now(),
                completed,
            },
        );
        drop(PendingIdempotencyGuard::new(
            Arc::clone(&state),
            "cancelled".into(),
            owner_id,
        ));
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.idempotency.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled pending entry should be released promptly");
    }

    #[test]
    fn asset_content_type_is_specific_when_known_and_binary_when_unknown() {
        assert_eq!(content_type_for_name("module.wasm"), "application/wasm");
        assert_eq!(content_type_for_name("font.woff2"), "font/woff2");
        assert_eq!(content_type_for_name("README"), "application/octet-stream");
        assert_eq!(
            content_type_for_name("data.weird"),
            "application/octet-stream"
        );
    }
}
