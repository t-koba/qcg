use anyhow::Result;
use axum::Json;
use axum::body::Body;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::Response;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::StreamExt as FuturesStreamExt;
use qcg_api::{AnswerPayload, ConfirmDecision};
use serde::Serialize;
use std::convert::Infallible;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;

use super::config::AppState;
use super::error::ApiHttpError;
use super::idempotency::with_idempotency;

pub(crate) async fn run_snapshot(
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

pub(crate) async fn delete_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiHttpError> {
    state
        .service
        .delete_run(&id)
        .await
        .map_err(ApiHttpError::from_api)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn read_run_bundle(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiHttpError> {
    let parts = state
        .service
        .run_bundle_parts(&id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let limits = state.artifact_limits;
    let (sender, receiver) = mpsc::channel::<Result<Vec<u8>, io::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter {
            sender: sender.clone(),
        };
        if let Err(error) = qcg_service::write_run_bundle_stream_with_limits(
            &parts.snapshot,
            &parts.inputs,
            &parts.journal,
            parts.outputs.as_ref(),
            &parts.verified,
            writer,
            &limits,
        ) {
            let _ = sender.blocking_send(Err(io::Error::other(error)));
        }
    });
    let stream = ReceiverStream::new(receiver);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{id}-bundle.zip\""),
        )
        .body(Body::from_stream(stream))
        .map_err(ApiHttpError::internal)
}

pub(crate) async fn answer_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((id, question_id)): Path<(String, String)>,
    Json(payload): Json<AnswerPayload>,
) -> Result<Response, ApiHttpError> {
    let mut body = Vec::with_capacity(question_id.len() + 1);
    body.extend_from_slice(question_id.as_bytes());
    body.push(0);
    body.extend_from_slice(&serde_json::to_vec(&payload).map_err(ApiHttpError::internal)?);
    with_idempotency(&state, &headers, "answer", &id, &body, false, || async {
        state
            .service
            .answer(id.clone(), question_id.clone(), payload)
            .await
            .map(|()| id.clone())
    })
    .await
}

pub(crate) async fn confirm_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((id, confirmation_id)): Path<(String, String)>,
    Json(decision): Json<ConfirmDecision>,
) -> Result<Response, ApiHttpError> {
    let mut body = Vec::with_capacity(confirmation_id.len() + 1);
    body.extend_from_slice(confirmation_id.as_bytes());
    body.push(0);
    body.extend_from_slice(&serde_json::to_vec(&decision).map_err(ApiHttpError::internal)?);
    with_idempotency(&state, &headers, "confirm", &id, &body, false, || async {
        state
            .service
            .confirm(id.clone(), confirmation_id.clone(), decision)
            .await
            .map(|()| id.clone())
    })
    .await
}

pub(crate) async fn cancel_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiHttpError> {
    with_idempotency(&state, &headers, "cancel", &id, &[], false, || async {
        state.service.cancel(id.clone()).await.map(|()| id.clone())
    })
    .await
}

pub(crate) async fn cancel_run_from_path(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
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
    cancel_run(State(state), headers, Path(id.to_string())).await
}

pub(crate) async fn dispatch_fallback(
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

pub(crate) fn unsafe_generator_asset_path(path: &str) -> bool {
    let Some((prefix, asset_path)) = path.split_once("/assets/") else {
        return false;
    };
    if !prefix.starts_with("/api/generators/") {
        return false;
    }
    let encoded = asset_path.to_ascii_lowercase();
    !qcg_policy::is_safe_relative_path(asset_path)
        || encoded.contains("%2e")
        || encoded.contains("%2f")
        || encoded.contains("%5c")
}

pub(crate) async fn run_events(
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

pub(crate) async fn run_artifacts(
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

pub(crate) fn weak_etag(value: &str) -> String {
    format!("W/\"{value}\"")
}

pub(crate) fn conditional_json<T: Serialize>(
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

pub(crate) async fn read_artifact(
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

pub(crate) async fn read_artifacts_zip(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiHttpError> {
    let run_dir = state
        .service
        .run_dir_for(&id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let limits = state.artifact_limits;
    let (sender, receiver) = mpsc::channel::<Result<Vec<u8>, io::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter {
            sender: sender.clone(),
        };
        if let Err(error) =
            qcg_service::write_artifacts_zip_stream_with_limits(&run_dir, writer, &limits)
        {
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

pub(crate) struct ChannelWriter {
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

pub(crate) async fn read_journal(
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

pub(crate) async fn read_cost_metrics(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<qcg_api::RunCostMetrics>, ApiHttpError> {
    state
        .service
        .run_cost_metrics(id)
        .await
        .map(Json)
        .map_err(ApiHttpError::from_api)
}

pub(crate) fn content_type_for_name(path: &str) -> &'static str {
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

pub(crate) fn content_disposition_attachment(path: &str) -> String {
    let file_name = path
        .rsplit('/')
        .next()
        .unwrap_or("artifact")
        .replace(['"', '\\', '\r', '\n'], "_");
    format!("attachment; filename=\"{file_name}\"")
}
