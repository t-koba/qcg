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
    AnswerPayload, ApiError, ConfirmDecision, GeneratorSummary, ProblemDetails, ProblemFieldError,
    RunListQuery, RunListResponse, RunSnapshot, StartRun,
};
use qcg_service::LocalQcgService;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::CorsLayer;

const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const IDEMPOTENCY_MAX_ENTRIES: usize = 1024;
const RUN_REQUEST_BODY_LIMIT: usize = qcg_types::MAX_FILE_INPUT_BYTES * 2;

#[cfg(test)]
const SERVER_ROUTES: &[(&str, &str, Option<&str>, Option<qcg_api::ResponseSchema>)] = &[
    ("get", "/healthz", None, None),
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
    pub cors_origins: Vec<String>,
}

#[derive(Debug)]
struct AppState {
    service: LocalQcgService,
    idempotency: tokio::sync::Mutex<BTreeMap<String, IdempotencyEntry>>,
}

#[derive(Debug)]
enum IdempotencyEntry {
    Pending {
        digest: String,
        created_at: Instant,
        completed: tokio::sync::watch::Sender<bool>,
    },
    Ready {
        digest: String,
        created_at: Instant,
        run_id: String,
    },
}

pub async fn serve_with_listener(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
) -> Result<SocketAddr> {
    let mut roots = vec![config.generators_dir.clone()];
    roots.extend(config.extra_generators_dirs.clone());
    let service = LocalQcgService::with_generator_roots(
        roots,
        config.runs_dir.clone(),
        config.providers_path.clone(),
    )?;
    let _gc_task = service.start_retention_gc();
    let state = Arc::new(AppState {
        service,
        idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
    });
    let shutdown_service = state.service.clone();
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/openapi.json", get(openapi))
        .route("/api/generators", get(list_generators))
        .route("/api/generators/{id}", get(describe_generator))
        .route(
            "/api/generators/{id}/assets/{*path}",
            get(read_generator_asset),
        )
        .route("/api/runs", get(list_runs).post(start_run))
        .route("/api/runs/{id}", get(run_snapshot))
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
                    header::CONTENT_TYPE,
                    header::HeaderName::from_static(IDEMPOTENCY_HEADER),
                ])
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::OPTIONS]),
        )
    } else {
        app
    };

    let actual_addr = listener.local_addr()?;
    tracing::info!(%actual_addr, "qcg server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_service))
        .await?;
    Ok(actual_addr)
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
    Ok(Json(state.service.list_generators().await))
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
    loop {
        let wait = {
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
                    Some(completed.subscribe())
                }
                None => {
                    if idempotency.len() >= IDEMPOTENCY_MAX_ENTRIES {
                        return Err(ApiHttpError::service_unavailable(
                            "too many idempotent requests are still in progress",
                        ));
                    }
                    let (completed, _) = tokio::sync::watch::channel(false);
                    idempotency.insert(
                        idempotency_key.clone(),
                        IdempotencyEntry::Pending {
                            digest: request_digest.clone(),
                            created_at: Instant::now(),
                            completed,
                        },
                    );
                    None
                }
            }
        };
        if let Some(mut completed) = wait {
            if !*completed.borrow() {
                let _ = completed.changed().await;
            }
            continue;
        }
        break;
    }
    let run_id = match state.service.start_run(req).await {
        Ok(run_id) => run_id,
        Err(error) => {
            let completed = {
                let mut idempotency = state.idempotency.lock().await;
                match idempotency.remove(&idempotency_key) {
                    Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
                    _ => None,
                }
            };
            if let Some(completed) = completed {
                let _ = completed.send(true);
            }
            return Err(ApiHttpError::from_api(error));
        }
    };
    let completed = {
        let mut idempotency = state.idempotency.lock().await;
        let completed = match idempotency.remove(&idempotency_key) {
            Some(IdempotencyEntry::Pending { completed, .. }) => Some(completed),
            _ => None,
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
        weak_etag(&format!("run-{}", snapshot.seq)),
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

async fn dispatch_fallback(
    State(state): State<Arc<AppState>>,
    method: Method,
    original_uri: OriginalUri,
) -> Result<Response, ApiHttpError> {
    if method == Method::POST
        && let Some(id) = cancel_run_id(original_uri.path())
    {
        return cancel_run(State(state), Path(id.to_string()))
            .await
            .map(IntoResponse::into_response);
    }
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

fn cancel_run_id(path: &str) -> Option<&str> {
    let id = path.strip_prefix("/api/runs/")?.strip_suffix(":cancel")?;
    (!id.is_empty() && !id.contains('/')).then_some(id)
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
    let (artifact, bytes) = state
        .service
        .read_artifact(id, path)
        .await
        .map_err(ApiHttpError::from_api)?;
    let content_type = artifact
        .mime
        .unwrap_or_else(|| content_type_for_name(&artifact.path).to_string());
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, content_disposition)
        .body(Body::from(bytes))
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
    fn cancel_suffix_route_accepts_exactly_one_run_id_segment() {
        assert_eq!(cancel_run_id("/api/runs/run-1:cancel"), Some("run-1"));
        assert_eq!(cancel_run_id("/api/runs/:cancel"), None);
        assert_eq!(cancel_run_id("/api/runs/a/b:cancel"), None);
        assert_eq!(cancel_run_id("/api/runs/run-1/cancel"), None);
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
                cors_origins: Vec::new(),
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
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
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
