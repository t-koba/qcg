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

use super::config::AppState;
use super::error::ApiHttpError;
use super::idempotency::{IdempotentCall, with_idempotency};

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
    with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "answer",
        target: &id,
        body: &body,
        created: false,
        reserved_run_id: Some(id.clone()),
        execute: |_| async {
            state
                .service
                .answer(id.clone(), question_id.clone(), payload)
                .await
                .map(|()| id.clone())
        },
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
    with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "confirm",
        target: &id,
        body: &body,
        created: false,
        reserved_run_id: Some(id.clone()),
        execute: |_| async {
            state
                .service
                .confirm(id.clone(), confirmation_id.clone(), decision)
                .await
                .map(|()| id.clone())
        },
    })
    .await
}

pub(crate) async fn cancel_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiHttpError> {
    with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "cancel",
        target: &id,
        body: &[],
        created: false,
        reserved_run_id: Some(id.clone()),
        execute: |_| async { state.service.cancel(id.clone()).await.map(|()| id.clone()) },
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
            let data = match serde_json::to_string(&event) {
                Ok(data) => data,
                Err(error) => {
                    tracing::warn!(seq = event.seq, %error, "dropping unserializable SSE event");
                    return None;
                }
            };
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
    // Verify size via metadata before allocating, then bound the read by
    // the manifest size + 1 so a swapped larger file never forces excess
    // allocation before the configured limits are enforced (C05).
    let metadata = tokio::fs::metadata(&resolved)
        .await
        .map_err(ApiHttpError::internal)?;
    if metadata.len() != artifact.bytes {
        return Err(ApiHttpError::internal(format!(
            "artifact `{}` bytes mismatch: manifest={}, actual={}",
            artifact.path,
            artifact.bytes,
            metadata.len()
        )));
    }
    if let Some(limit) = state.artifact_limits.max_bytes
        && metadata.len() > limit
    {
        return Err(ApiHttpError::from_api(qcg_api::ApiError::TooLarge {
            actual_bytes: metadata.len() as usize,
            limit_bytes: limit as usize,
        }));
    }
    // Streamed verification on the open file description (constant
    // memory): hash and length are checked before delivery, then the SAME
    // description rewinds for the response body, so a path swap between
    // verify and serve cannot substitute bytes.
    let mut file = tokio::fs::File::open(&resolved)
        .await
        .map_err(ApiHttpError::internal)?;
    {
        use sha2::Digest as _;
        use tokio::io::AsyncReadExt as _;
        let mut hasher = sha2::Sha256::new();
        let mut seen: u64 = 0;
        // Manifest size + 1 detects post-stat growth without over-allocating.
        let mut remaining = artifact.bytes.saturating_add(1);
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let limit = (chunk.len() as u64).min(remaining) as usize;
            if limit == 0 {
                break;
            }
            let read = file
                .read(&mut chunk[..limit])
                .await
                .map_err(ApiHttpError::internal)?;
            if read == 0 {
                break;
            }
            hasher.update(&chunk[..read]);
            seen = seen.saturating_add(read as u64);
            remaining = remaining.saturating_sub(read as u64);
        }
        verify_artifact_measurement(&artifact, seen, &hex::encode(hasher.finalize()))
            .map_err(ApiHttpError::internal)?;
    }
    {
        use tokio::io::AsyncSeekExt as _;
        file.rewind().await.map_err(ApiHttpError::internal)?;
    }
    let content_type = artifact
        .mime
        .unwrap_or_else(|| content_type_for_name(&artifact.path).to_string());
    let stream = tokio_util::io::ReaderStream::new(file);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, content_disposition)
        .header(header::CONTENT_LENGTH, artifact.bytes)
        .body(Body::from_stream(stream))
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
    // Constant-memory delivery: the bound (when configured) was already
    // enforced against the file size at open, so no path allocates beyond
    // its checked bound. Live journals may extend mid-stream; the body is
    // raw ndjson bytes as written.
    let stream = state
        .service
        .open_journal_stream(id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let body = match stream.limit {
        Some(limit) => {
            use tokio::io::AsyncReadExt as _;
            let mut bytes = Vec::new();
            stream
                .file
                .take(limit.saturating_add(1) as u64)
                .read_to_end(&mut bytes)
                .await
                .map_err(ApiHttpError::internal)?;
            if bytes.len() > limit {
                return Err(ApiHttpError::from_api(qcg_api::ApiError::TooLarge {
                    actual_bytes: bytes.len(),
                    limit_bytes: limit,
                }));
            }
            Body::from(bytes)
        }
        None => Body::from_stream(tokio_util::io::ReaderStream::new(stream.file)),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(body)
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

/// Verify a singly-served artifact against its manifest before delivery.
/// Size alone cannot detect a same-length post-completion swap, so the
/// sha256 is always compared, matching the ZIP/bundle verification.
/// Single comparison contract for artifact delivery, shared by buffered
/// and streaming verifiers so the two paths cannot disagree.
pub(crate) fn verify_artifact_measurement(
    artifact: &qcg_types::OutputArtifact,
    actual_len: u64,
    actual_sha256: &str,
) -> Result<(), String> {
    if actual_len != artifact.bytes {
        return Err(format!(
            "artifact `{}` bytes mismatch: manifest={}, actual={}",
            artifact.path, artifact.bytes, actual_len
        ));
    }
    if actual_sha256 != artifact.sha256 {
        return Err(format!(
            "artifact `{}` content mismatch: manifest sha256 does not match file",
            artifact.path,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest as _;

    fn artifact_for(bytes: &[u8]) -> qcg_types::OutputArtifact {
        qcg_types::OutputArtifact {
            path: "reports/result.txt".into(),
            sha256: hex::encode(sha2::Sha256::digest(bytes)),
            bytes: bytes.len() as u64,
            label: "Result".into(),
            required: true,
            mime: None,
            description: String::new(),
            preview: Default::default(),
        }
    }

    fn measured(bytes: &[u8]) -> (u64, String) {
        (bytes.len() as u64, hex::encode(sha2::Sha256::digest(bytes)))
    }

    #[test]
    fn single_artifact_delivery_rejects_same_size_swap() {
        let original = b"result-v1";
        let artifact = artifact_for(original);
        let (len, digest) = measured(original);
        verify_artifact_measurement(&artifact, len, &digest).expect("matching content should pass");
        let swapped = b"result-v2";
        assert_eq!(swapped.len(), original.len());
        let (swapped_len, swapped_digest) = measured(swapped);
        let error = verify_artifact_measurement(&artifact, swapped_len, &swapped_digest)
            .expect_err("same-size content swap must be rejected");
        assert!(error.contains("content mismatch"));
        let truncated = b"result-";
        let (truncated_len, truncated_digest) = measured(truncated);
        let error = verify_artifact_measurement(&artifact, truncated_len, &truncated_digest)
            .expect_err("size change must be rejected");
        assert!(error.contains("bytes mismatch"));
    }
}
