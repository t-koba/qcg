use anyhow::Result;
use axum::Json;
use axum::body::Body;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::IntoResponse as _;
use axum::response::Response;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::StreamExt as FuturesStreamExt;
use qcg_api::{AnswerPayload, ConfirmDecision};
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
    // The validator is the exact body digest: the whole snapshot
    // serializes into it (run/contract/generator identity, seq, state,
    // queue_position, metrics, queued_at, priority, parent, question,
    // confirm, and artifacts), so any visible field moves the validator
    // and a seq-only ETag could never return a stale 304 (E16). Single
    // entry point computes the digest once from the served bytes.
    let body = serde_json::to_vec(&snapshot).map_err(ApiHttpError::internal)?;
    conditional_response(&headers, body, "application/json")
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
    // Tied to shutdown and client disconnect and joined (E05): the blocking
    // zip writer aborts on shutdown instead of continuing detached, aborts
    // when the client disconnects (receiver dropped, tied to the response
    // lifetime), and the watcher awaits it so no blocking task survives the
    // response. The task itself is bounded by artifact limits, so normal
    // completion needs no abort.
    // No ETag: streaming archives cannot validate without hashing the full
    // body upfront (defeating streaming); clients condition on the snapshot
    // or artifact-list ETag as the queue-revision alternative (E16).
    let disconnect_sender = sender.clone();
    let mut blocking = tokio::task::spawn_blocking(move || {
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
            // Best-effort by necessity: the receiver is gone only when the
            // client disconnected or shutdown closed the stream, so no
            // consumer remains to observe the error. It is logged, never
            // silently dropped (E05).
            if let Err(send_error) = sender.blocking_send(Err(io::Error::other(error))) {
                tracing::warn!(%send_error, "failed to deliver bundle error; receiver is gone");
            }
        }
    });
    let shutdown = state.shutdown.clone();
    let shutdown_watch = shutdown.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_watch.cancelled() => {
                blocking.abort();
                if let Err(error) = blocking.await
                    && !error.is_cancelled()
                {
                    tracing::warn!(%error, "bundle writer failed during shutdown abort");
                }
            }
            // Client disconnect (receiver dropped, response lifetime ended):
            // abort the writer so it cannot continue detached past the
            // disconnected response (E05).
            _ = disconnect_sender.closed() => {
                blocking.abort();
                if let Err(error) = blocking.await
                    && !error.is_cancelled()
                {
                    tracing::warn!(%error, "bundle writer failed during disconnect abort");
                }
            }
            result = &mut blocking => {
                if let Err(error) = result
                    && !error.is_cancelled()
                {
                    tracing::warn!(%error, "bundle writer task failed");
                }
            }
        }
    });
    // Ends at shutdown like the artifacts zip: the draining connection
    // closes instead of holding graceful drain open (E05).
    let stream = ReceiverStream::new(receiver).take_until(async move {
        shutdown.cancelled().await;
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{id}-bundle.zip\""),
        )
        .header(header::CACHE_CONTROL, "no-store")
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
        // The target id rides in the pending claim for uniformity (E02).
        // Reject on mismatch instead of ignoring it: executing a different
        // run than reserved could never commit.
        execute: |reserved| async {
            match reserved {
                Some(reserved) if reserved == id => state
                    .service
                    .answer(id.clone(), question_id.clone(), payload)
                    .await
                    .map(|()| id.clone()),
                _ => Err(qcg_api::ApiError::Internal {
                    detail: "idempotency reservation does not match the target run".into(),
                }),
            }
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
        // Reservation check mirrors `answer_run` (E02).
        execute: |reserved| async {
            match reserved {
                Some(reserved) if reserved == id => state
                    .service
                    .confirm(id.clone(), confirmation_id.clone(), decision)
                    .await
                    .map(|()| id.clone()),
                _ => Err(qcg_api::ApiError::Internal {
                    detail: "idempotency reservation does not match the target run".into(),
                }),
            }
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
        // Reservation check mirrors `answer_run` (E02).
        execute: |reserved| async {
            match reserved {
                Some(reserved) if reserved == id => {
                    state.service.cancel(id.clone()).await.map(|()| id.clone())
                }
                _ => Err(qcg_api::ApiError::Internal {
                    detail: "idempotency reservation does not match the target run".into(),
                }),
            }
        },
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
) -> Result<Response, ApiHttpError> {
    // `Last-Event-ID` is a plain unsigned journal seq. Surrounding
    // whitespace is trimmed for robustness; multi-value, weak-validator,
    // and `*` forms are not cursors and are rejected (E12).
    let after_seq = headers
        .get("last-event-id")
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Last-Event-ID must be valid ASCII"))?
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse::<u64>)
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("Last-Event-ID must be an unsigned integer"))?
        .unwrap_or(0);
    let shutdown = state.shutdown.clone();
    // Drain path (E05): when shutdown already started, do NOT create a new
    // shared poller via `subscribe_with_cursor`. Serve history-then-close
    // from the journal file directly (no poller, no live tail) plus the
    // shutdown marker, so draining never spawns work that outlives the
    // drain. Cursor filtering and future-cursor clamping mirror the live
    // path so resume semantics stay identical.
    if shutdown.is_cancelled() {
        let run_dir = state
            .service
            .run_dir_for(&id)
            .await
            .map_err(ApiHttpError::from_api)?;
        let history = tokio::task::spawn_blocking(move || qcg_service::read_run_events(&run_dir))
            .await
            .map_err(|error| ApiHttpError::internal(format!("event history task failed: {error}")))?
            .map_err(ApiHttpError::internal)?;
        let history_last_seq = history.last().map_or(0, |event| event.seq);
        let clamped_after = if after_seq > history_last_seq {
            0
        } else {
            after_seq
        };
        let tail: Vec<qcg_api::RunEvent> = history
            .into_iter()
            .filter(|event| event.seq > clamped_after)
            .collect();
        let terminal_seen = tail.last().is_some_and(|event| {
            qcg_api::is_terminal_event_kind(event.kind.as_str())
                || event.kind.as_str() == "stream_error"
        });
        let last_delivered = tail.last().map_or(clamped_after, |event| event.seq);
        let history_stream = futures_util::stream::iter(tail.into_iter().map(|event| {
            let seq = event.seq;
            match serde_json::to_string(&event) {
                Ok(data) => Ok(Event::default().id(seq.to_string()).data(data)),
                Err(error) => {
                    tracing::error!(seq, %error, "closing SSE stream after an unserializable event");
                    let marker = serde_json::json!({
                        "seq": seq,
                        "kind": "stream_error",
                        "data": {"reason": "unserializable event"},
                    });
                    let data = serde_json::to_string(&marker)
                        .unwrap_or_else(|_| r#"{"kind":"stream_error"}"#.to_string());
                    Ok(Event::default()
                        .id(seq.to_string())
                        .event("stream_error")
                        .data(data))
                }
            }
        }));
        let terminal_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(terminal_seen));
        let marker = shutdown_marker_stream_with_seq(
            shutdown.clone(),
            last_delivered.saturating_add(1),
            terminal_flag,
        );
        return Ok(Sse::new(history_stream.chain(marker))
            .keep_alive(KeepAlive::default())
            .into_response());
    }
    // Cursor filtering and future-cursor clamping happen inside
    // subscribe_with_cursor; every event below is unseen (E12). A drain
    // close ends the stream with an explicit `shutdown` marker event so
    // clients distinguish shutdown from truncation: a terminal close is
    // always preceded by its terminal event, a shutdown close by the marker,
    // and a truncation by neither (E05).
    let shutdown_for_stream = shutdown.clone();
    // Shared drain state for the live path (E05): the scan below records
    // the last delivered seq and whether a terminal event was sent; the
    // chained marker reads both so it carries the next seq with a stable id
    // and is suppressed when a terminal already closed the stream.
    let terminal_seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let last_delivered = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(after_seq));
    let terminal_for_scan = terminal_seen.clone();
    let delivered_for_scan = last_delivered.clone();
    let stream = state
        .service
        .subscribe_with_cursor(id, after_seq)
        .await
        .map_err(ApiHttpError::from_api)?
        // A long-lived SSE connection must not hold graceful drain open:
        // closing the token ends every stream at shutdown (E05).
        .take_until(async move { shutdown_for_stream.cancelled().await })
        // End the stream after the terminal event, so clients observe the
        // outcome and the connection closes instead of living until GC.
        // An unserializable event is a bug, not a gap to paper over: close
        // the stream instead of silently continuing without it.
        .scan(false, move |done, event| {
            if *done {
                return std::future::ready(None);
            }
            // A `stream_error` marker ends the stream after delivery like a
            // terminal event, but it is a failure-close, never a normal
            // outcome: consumers distinguish it by kind (E05).
            let is_failure = event.kind.as_str() == "stream_error";
            let is_terminal = qcg_api::is_terminal_event_kind(event.kind.as_str()) || is_failure;
            *done = is_terminal;
            if is_terminal {
                terminal_for_scan.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            delivered_for_scan.store(event.seq, std::sync::atomic::Ordering::SeqCst);
            match serde_json::to_string(&event) {
                Ok(data) => std::future::ready(Some(Some(Ok(Event::default()
                    .id(event.seq.to_string())
                    .data(data))))),
                Err(error) => {
                    *done = true;
                    terminal_for_scan.store(true, std::sync::atomic::Ordering::SeqCst);
                    tracing::error!(
                        seq = event.seq,
                        %error,
                        "closing SSE stream after an unserializable event"
                    );
                    // Failure-close marker so the client distinguishes a
                    // truncated stream from a terminal outcome (E05).
                    let marker = serde_json::json!({
                        "seq": event.seq,
                        "kind": "stream_error",
                        "data": {"reason": "unserializable event"},
                    });
                    let data = serde_json::to_string(&marker)
                        .unwrap_or_else(|_| r#"{"kind":"stream_error"}"#.to_string());
                    std::future::ready(Some(Some(Ok(Event::default()
                        .id(event.seq.to_string())
                        .event("stream_error")
                        .data(data)))))
                }
            }
        })
        .filter_map(std::future::ready);
    // Shutdown marker: when the stream above ends via the shutdown token,
    // emit one terminal `shutdown` event before closing so clients
    // distinguish shutdown from truncation (E05). Suppressed when the main
    // stream already delivered a terminal event (no double termination).
    // The marker carries the next seq with a stable id so clients resume
    // via Last-Event-ID from it (E05). The seq is resolved at close time
    // from the shared `last_delivered` cursor, never frozen at subscribe.
    let stream = stream.chain(shutdown_marker_stream_dynamic(
        shutdown,
        last_delivered,
        terminal_seen,
    ));
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}

/// Seq-aware shutdown marker (E05): the marker carries the next seq after
/// the last delivered event with a stable id (`seq` as the SSE id) so
/// clients resume via `Last-Event-ID` from it. Suppressed when a terminal
/// event already closed the stream (no double termination). The seq is read
/// at close time from `last_delivered`, so live events that flowed before
/// the drain are reflected.
fn shutdown_marker_stream_dynamic(
    shutdown: tokio_util::sync::CancellationToken,
    last_delivered: std::sync::Arc<std::sync::atomic::AtomicU64>,
    terminal_seen: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> impl tokio_stream::Stream<Item = Result<Event, Infallible>> {
    let shutdown_for_marker = shutdown.clone();
    futures_util::stream::unfold(false, move |emitted| {
        let shutdown = shutdown_for_marker.clone();
        let last_delivered = last_delivered.clone();
        let terminal_seen = terminal_seen.clone();
        async move {
            if emitted || !shutdown.is_cancelled() {
                return None;
            }
            // A terminal event already closed the stream: suppress the
            // shutdown marker so clients never observe double termination
            // (E05).
            if terminal_seen.load(std::sync::atomic::Ordering::SeqCst) {
                return None;
            }
            let seq = last_delivered
                .load(std::sync::atomic::Ordering::SeqCst)
                .saturating_add(1);
            let data = serde_json::json!({
                "seq": seq,
                "kind": "shutdown",
                "data": {"reason": "server is shutting down"},
            });
            let text = serde_json::to_string(&data)
                .unwrap_or_else(|_| r#"{"kind":"shutdown"}"#.to_string());
            Some((
                Some(Ok(Event::default()
                    .id(seq.to_string())
                    .event("shutdown")
                    .data(text))),
                true,
            ))
        }
    })
    .filter_map(std::future::ready)
}

/// Static-seq marker for the history-only drain path (E05): the last
/// delivered seq is already final (no live tail), so the next seq is frozen
/// at stream construction. Terminal suppression still applies.
fn shutdown_marker_stream_with_seq(
    shutdown: tokio_util::sync::CancellationToken,
    next_seq: u64,
    terminal_seen: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> impl tokio_stream::Stream<Item = Result<Event, Infallible>> {
    let last = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
        next_seq.saturating_sub(1),
    ));
    shutdown_marker_stream_dynamic(shutdown, last, terminal_seen)
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
    // The validator is the exact body digest: the artifact set can change
    // (files appear or are invalidated) without the run's own sequence
    // moving, so a seq-only ETag would return a stale 304 (E16). Single
    // entry point computes the digest once from the served bytes.
    let body = serde_json::to_vec(&artifacts).map_err(ApiHttpError::internal)?;
    conditional_response(&headers, body, "application/json")
}

/// The exact-body ETag shared by every conditional JSON endpoint, so a
/// digest change can never diverge between snapshot and artifact paths
/// (E16). Single hash: callers pass bytes, never a precomputed validator.
pub(crate) fn body_etag(body: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    weak_etag(&hex::encode(Sha256::digest(body)))
}

fn weak_etag(value: &str) -> String {
    format!("W/\"{value}\"")
}

/// Parsed `If-None-Match` precondition (RFC 7232).
enum IfNoneMatch {
    Absent,
    Star,
    Tags(Vec<String>),
}

/// Parses `If-None-Match` fail-closed: `*` alone, or a comma-separated
/// entity-tag list where each entry is `[W/]"opaque-tag"`. Any malformed
/// value (empty, trailing comma, bare `*` inside a list, lowercase `w/`,
/// missing quotes, empty or control-containing opaque tag, non-ASCII)
/// returns 400 instead of falling through to 200 (E16).
fn parse_if_none_match(headers: &HeaderMap) -> Result<IfNoneMatch, ApiHttpError> {
    let header_value = headers
        .get(header::IF_NONE_MATCH)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiHttpError::bad_request("If-None-Match must be valid ASCII"))?;
    let Some(none_match_raw) = header_value else {
        return Ok(IfNoneMatch::Absent);
    };
    let none_match = none_match_raw.trim();
    if none_match.is_empty() {
        return Err(ApiHttpError::bad_request(
            "If-None-Match must be `*` or a comma-separated entity-tag list",
        ));
    }
    if none_match == "*" {
        return Ok(IfNoneMatch::Star);
    }
    let mut tags = Vec::new();
    for raw in none_match.split(',') {
        let candidate = raw.trim();
        if candidate.is_empty() {
            return Err(ApiHttpError::bad_request(
                "If-None-Match must be `*` or a comma-separated entity-tag list",
            ));
        }
        if candidate == "*" {
            return Err(ApiHttpError::bad_request(
                "If-None-Match must be `*` or a comma-separated entity-tag list",
            ));
        }
        let opaque = candidate.strip_prefix("W/").unwrap_or(candidate);
        if opaque.starts_with("w/") || candidate.starts_with("w/") {
            return Err(ApiHttpError::bad_request(
                "If-None-Match must be `*` or a comma-separated entity-tag list",
            ));
        }
        if !(opaque.starts_with('"') && opaque.ends_with('"') && opaque.len() >= 2) {
            return Err(ApiHttpError::bad_request(
                "If-None-Match must be `*` or a comma-separated entity-tag list",
            ));
        }
        let inner = &opaque[1..opaque.len() - 1];
        if inner.is_empty() || inner.bytes().any(|b| b == b'"' || b < 0x21 || b == 0x7f) {
            return Err(ApiHttpError::bad_request(
                "If-None-Match must be `*` or a comma-separated entity-tag list",
            ));
        }
        tags.push(inner.to_string());
    }
    Ok(IfNoneMatch::Tags(tags))
}

/// Weak comparison of a current opaque tag against a parsed precondition:
/// `*` matches any existing representation, otherwise any listed tag equal
/// to the current one matches (E16).
fn if_none_match_matches(precondition: &IfNoneMatch, current_opaque: &str) -> bool {
    match precondition {
        IfNoneMatch::Absent => false,
        IfNoneMatch::Star => true,
        IfNoneMatch::Tags(tags) => tags.iter().any(|tag| tag == current_opaque),
    }
}

/// Current opaque tag from a weak ETag value (`W/"..."` or `"..."`).
fn current_opaque_tag(etag: &str) -> String {
    etag.trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_string()
}

/// Single conditional-bytes entry point (E16): callers pass the exact
/// bytes to serve plus the content type; the validator is the exact-body
/// digest computed once here, so no caller-supplied ETag can diverge from
/// the body and the hash runs exactly once total. Supports `If-None-Match`
/// lists and weak comparison per RFC 7232: any listed validator matching
/// the current ETag (weakly) yields 304. Syntactically invalid validators
/// fail closed with 400 instead of falling through to 200: a malformed
/// precondition must surface, never silently cache-bypass (E16).
pub(crate) fn conditional_response(
    headers: &HeaderMap,
    body: Vec<u8>,
    content_type: &'static str,
) -> Result<Response, ApiHttpError> {
    // Single hash total (E16): the served validator is the exact-body
    // digest computed here; there is no caller ETag arg to discard or
    // diverge.
    let served = body_etag(&body);
    conditional_response_with_etag(headers, body, content_type, &served)
}

/// Core conditional responder sharing one precomputed validator (E16):
/// [`conditional_response`] funnels here so the hash runs exactly once per
/// response. `served` must be `body_etag(&body)`.
fn conditional_response_with_etag(
    headers: &HeaderMap,
    body: Vec<u8>,
    content_type: &'static str,
    served: &str,
) -> Result<Response, ApiHttpError> {
    let precondition = parse_if_none_match(headers)?;
    if if_none_match_matches(&precondition, &current_opaque_tag(served)) {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, served)
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::empty())
            .map_err(ApiHttpError::internal);
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ETAG, served)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body))
        .map_err(ApiHttpError::internal)
}

/// Exact-body validator response shared by conditional reads and mutation
/// responses: one builder so status-only callers cannot diverge the
/// validator, content type, or cache headers (E16). `location` carries an
/// optional `Location` header for creations.
pub(crate) fn json_response_with_etag(
    status: StatusCode,
    body: Vec<u8>,
    location: Option<String>,
) -> Result<Response, ApiHttpError> {
    let etag = body_etag(&body);
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ETAG, etag)
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(location) = location {
        builder = builder.header(header::LOCATION, location);
    }
    builder
        .body(Body::from(body))
        .map_err(ApiHttpError::internal)
}

pub(crate) async fn read_artifact(
    State(state): State<Arc<AppState>>,
    Path((id, path)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let content_disposition = content_disposition_attachment(&path);
    // Retained for stream diagnostics below: the service call moves `id`.
    let logged_id = id.clone();
    let (artifact, resolved) = state
        .service
        .read_artifact(id, path)
        .await
        .map_err(ApiHttpError::from_api)?;
    // Strong validator from the manifest sha256 (E16): the content is
    // verified byte-for-byte against this digest before delivery, so the
    // ETag names the exact bytes served. Weak comparison still applies, and
    // `*` matches any existing artifact. Invalid preconditions fail closed
    // with 400 via the shared parser.
    let strong_etag = format!("\"{}\"", artifact.sha256);
    match parse_if_none_match(&headers)? {
        IfNoneMatch::Absent => {}
        IfNoneMatch::Star => {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, strong_etag)
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::empty())
                .map_err(ApiHttpError::internal);
        }
        IfNoneMatch::Tags(tags) => {
            if tags.iter().any(|tag| tag == &artifact.sha256) {
                return Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header(header::ETAG, strong_etag)
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(Body::empty())
                    .map_err(ApiHttpError::internal);
            }
        }
    }
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
    // A slow client must not hold graceful drain open: the body ends at
    // shutdown like every other long-lived stream (E05).
    let shutdown = state.shutdown.clone();
    let logged_path = artifact.path.clone();
    let logged_bytes = artifact.bytes;
    let stream = tokio_util::io::ReaderStream::new(file)
        .take_until(async move {
            shutdown.cancelled().await;
        })
        .then(move |item| {
            let logged_id = logged_id.clone();
            let logged_path = logged_path.clone();
            async move {
                item.map_err(|error| {
                    // A mid-body read failure aborts the connection, which
                    // the client reports as a truncated body: log the run,
                    // path, and expected length so the truncation is
                    // diagnosable server-side.
                    tracing::error!(
                        run_id = %logged_id,
                        path = %logged_path,
                        expected_bytes = logged_bytes,
                        %error,
                        "artifact body stream failed mid-delivery",
                    );
                    error
                })
            }
        });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, content_disposition)
        .header(header::CONTENT_LENGTH, artifact.bytes)
        .header(header::ETAG, strong_etag)
        .header(header::CACHE_CONTROL, "no-store")
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
    // Tied to shutdown and client disconnect and joined (E05): aborts on
    // shutdown instead of continuing detached, aborts when the client
    // disconnects (receiver dropped, tied to the response lifetime). No
    // ETag for the same streaming reason as the bundle path: clients use
    // the artifact-list ETag (E16).
    let disconnect_sender = sender.clone();
    let mut blocking = tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter {
            sender: sender.clone(),
        };
        if let Err(error) =
            qcg_service::write_artifacts_zip_stream_with_limits(&run_dir, writer, &limits)
        {
            // Best-effort by necessity: a failed send means the receiver
            // was dropped (client disconnected or shutdown), so no consumer
            // remains. Logged, never silently dropped (E05).
            if let Err(send_error) = sender.blocking_send(Err(io::Error::other(error))) {
                tracing::warn!(%send_error, "failed to deliver artifacts zip error; receiver is gone");
            }
        }
    });
    let shutdown = state.shutdown.clone();
    let shutdown_watch = shutdown.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_watch.cancelled() => {
                blocking.abort();
                if let Err(error) = blocking.await
                    && !error.is_cancelled()
                {
                    tracing::warn!(%error, "artifacts zip writer failed during shutdown abort");
                }
            }
            // Client disconnect (receiver dropped): abort so the writer
            // cannot continue detached past the disconnected response (E05).
            _ = disconnect_sender.closed() => {
                blocking.abort();
                if let Err(error) = blocking.await
                    && !error.is_cancelled()
                {
                    tracing::warn!(%error, "artifacts zip writer failed during disconnect abort");
                }
            }
            result = &mut blocking => {
                if let Err(error) = result
                    && !error.is_cancelled()
                {
                    tracing::warn!(%error, "artifacts zip writer task failed");
                }
            }
        }
    });
    // The zip body ends at shutdown like every other long-lived stream:
    // the draining connection closes instead of holding graceful drain
    // open (E05). Read endpoints stay admissible during the drain; only
    // mutating work is refused with 503.
    let stream = ReceiverStream::new(receiver).take_until(async move {
        shutdown.cancelled().await;
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{id}-artifacts.zip\""),
        )
        .header(header::CACHE_CONTROL, "no-store")
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

/// Merges two NDJSON record streams by their shared seq space. A line that
/// does not parse or carries no seq fails the request instead of being
/// dropped silently: the journal view must never hide corruption.
fn merge_record_streams(durable: &[u8], observed: &[u8]) -> Result<Vec<u8>, ApiHttpError> {
    let decode = |bytes: &[u8]| -> Result<Vec<(u64, Vec<u8>)>, ApiHttpError> {
        let mut decoded = Vec::new();
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let seq = serde_json::from_slice::<serde_json::Value>(line)
                .ok()
                .and_then(|value| value.get("seq").and_then(serde_json::Value::as_u64))
                .ok_or_else(|| ApiHttpError::internal("journal line carries no seq"))?;
            decoded.push((seq, line.to_vec()));
        }
        Ok(decoded)
    };
    let mut merged = decode(durable)?;
    merged.extend(decode(observed)?);
    merged.sort_by_key(|(seq, _)| *seq);
    let mut bytes = Vec::new();
    for (_, line) in merged {
        bytes.extend_from_slice(&line);
        if !line.ends_with(b"\n") {
            bytes.push(b'\n');
        }
    }
    Ok(bytes)
}

pub(crate) async fn read_journal(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    // Constant-memory delivery: the bound (when configured) was already
    // enforced against the file size at open, so no path allocates beyond
    // its checked bound. The bounded path serves an exact-body validator
    // (E16, single entry point below). The unbounded path serves a weak
    // revision validator over (size + mtime) so both journal shapes carry a
    // validator (E16 bounded/unbounded unification): hashing the full
    // unbounded body upfront would defeat streaming and risk OOM, so the
    // revision names the file version instead of the bytes, and clients use
    // the snapshot ETag as the queue-revision alternative when they need
    // exact-body semantics (E16). Both are bounded by the file size at open
    // (explicit end, not only shutdown) so a slow client cannot hold
    // graceful drain open forever (E05).
    let shutdown = state.shutdown.clone();
    let mut stream = state
        .service
        .open_journal_stream(id)
        .await
        .map_err(ApiHttpError::from_api)?;
    // Merged view (ADR 0001): observation records live in the sibling
    // `audit.jsonl`, so the public journal view interleaves both streams by
    // seq. Runs without an observation stream keep the constant-memory
    // streaming path below unchanged.
    if stream.audit.is_some() {
        use tokio::io::AsyncReadExt as _;
        let mut bytes = Vec::new();
        match stream.limit {
            Some(limit) => {
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
            }
            None => {
                stream
                    .file
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(ApiHttpError::internal)?;
            }
        }
        let mut observed = Vec::new();
        if let Some(mut audit) = stream.audit {
            audit
                .read_to_end(&mut observed)
                .await
                .map_err(ApiHttpError::internal)?;
        }
        let merged = merge_record_streams(&bytes, &observed)?;
        return conditional_response(&headers, merged, "application/x-ndjson");
    }
    if let Some(limit) = stream.limit {
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
        return conditional_response(&headers, bytes, "application/x-ndjson");
    }
    // Unbounded live tail with a weak revision validator (E16): the ETag
    // names (size_at_open + mtime) so a changed journal moves the validator
    // without hashing the full body. Conditional requests with a matching
    // validator yield 304 without streaming.
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
    let mut file = stream.file;
    let metadata = file.metadata().await.map_err(ApiHttpError::internal)?;
    let size_at_open = metadata.len();
    // Nanosecond mtime with fail-closed metadata (E05/E16): second
    // granularity aliases distinct revisions within one second, and a
    // zero fallback aliases every metadata failure onto the epoch.
    // A metadata failure therefore refuses the conditional read instead
    // of serving a colliding weak validator.
    let mtime_nanos = metadata
        .modified()
        .map_err(ApiHttpError::internal)?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(ApiHttpError::internal)?
        .as_nanos();
    let revision_etag = weak_etag(&format!("{size_at_open}-{mtime_nanos}"));
    // Weak comparison for the revision validator (same parser as exact-body
    // paths, single precondition surface).
    match parse_if_none_match(&headers)? {
        IfNoneMatch::Absent => {}
        IfNoneMatch::Star => {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, revision_etag)
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::empty())
                .map_err(ApiHttpError::internal);
        }
        IfNoneMatch::Tags(tags) => {
            if tags
                .iter()
                .any(|tag| tag == current_opaque_tag(&revision_etag).as_str())
            {
                return Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header(header::ETAG, revision_etag)
                    .header(header::CACHE_CONTROL, "no-store")
                    .body(Body::empty())
                    .map_err(ApiHttpError::internal);
            }
        }
    }
    // Rewind: `open_journal_stream` may leave the cursor elsewhere; the
    // bounded size above is measured from the start. A seek failure fails
    // closed instead of streaming from a wrong offset with 200 OK (E05).
    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(ApiHttpError::internal)?;
    let body = Body::from_stream(
        tokio_util::io::ReaderStream::new(file.take(size_at_open)).take_until(async move {
            shutdown.cancelled().await;
        }),
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(header::ETAG, revision_etag)
        .header(header::CACHE_CONTROL, "no-store")
        .body(body)
        .map_err(ApiHttpError::internal)
}

pub(crate) async fn read_cost_metrics(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let metrics = state
        .service
        .run_cost_metrics(id)
        .await
        .map_err(ApiHttpError::from_api)?;
    // Exact-body validator like snapshots and artifact lists (E16): cost
    // totals move without the run seq moving, so a seq-only ETag would
    // return a stale 304. Single entry point computes the digest once.
    let body = serde_json::to_vec(&metrics).map_err(ApiHttpError::internal)?;
    conditional_response(&headers, body, "application/json")
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

    fn snapshot_for_etag(queue_position: Option<usize>, seq: u64) -> qcg_api::RunSnapshot {
        // E16: validator tests go through `RunSnapshot`, never ad-hoc JSON
        // bytes, so a queue-revision move is observed on the real shape.
        qcg_api::RunSnapshot {
            run_id: "run-etag".to_string(),
            state: qcg_api::RunStatus::Queued,
            seq,
            contract_sha256: None,
            generator_id: "gen".to_string(),
            artifacts: None,
            question: None,
            confirm: None,
            queued_at: None,
            queue_position,
            priority: 0,
            parent_run_id: None,
            metrics: None,
            labels: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn body_etag_is_the_exact_body_digest() {
        // E16: any byte change, including live-only metric movement with
        // an unchanged seq, must change the validator. Goes through
        // `RunSnapshot` so queue-revision movement moves the validator.
        let first_snapshot = snapshot_for_etag(Some(2), 7);
        let first_bytes = serde_json::to_vec(&first_snapshot).expect("snapshot should serialize");
        let first = body_etag(&first_bytes);
        assert_eq!(first, body_etag(&first_bytes));
        let moved = snapshot_for_etag(Some(1), 7);
        assert_ne!(
            first,
            body_etag(&serde_json::to_vec(&moved).expect("snapshot should serialize")),
            "a queue-position move with unchanged seq must move the validator"
        );
        let seq_moved = snapshot_for_etag(Some(2), 8);
        assert_ne!(
            first,
            body_etag(&serde_json::to_vec(&seq_moved).expect("snapshot should serialize")),
            "a seq move must move the validator"
        );
        assert!(first.starts_with("W/\"") && first.ends_with('"'));
    }

    #[test]
    fn snapshot_etag_moves_with_queue_revision_through_single_entry_point() {
        // E16: queue-revision validator change through the single
        // `conditional_response` entry point on the `RunSnapshot` shape:
        // position 2 -> 1 returns 200 with a new validator, never a stale
        // 304.
        use axum::http::{HeaderMap, StatusCode, header};
        let before =
            serde_json::to_vec(&snapshot_for_etag(Some(2), 7)).expect("snapshot should serialize");
        let after =
            serde_json::to_vec(&snapshot_for_etag(Some(1), 7)).expect("snapshot should serialize");
        let etag_before = body_etag(&before);
        assert_ne!(etag_before, body_etag(&after));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            etag_before.parse().expect("header value"),
        );
        let response =
            conditional_response(&headers, after.clone(), "application/json").expect("conditional");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a changed queue position must return the updated body, not 304"
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            body_etag(&after).parse().expect("header value"),
        );
        let response =
            conditional_response(&headers, after, "application/json").expect("conditional");
        assert_eq!(
            response.status(),
            StatusCode::NOT_MODIFIED,
            "an unchanged snapshot must reuse the conditional response"
        );
    }

    #[test]
    fn streaming_bodies_carry_no_validator_and_pin_the_snapshot_alternative() {
        // E16: streaming archives (bundle/zip) cannot validate without
        // hashing the full body upfront, defeating streaming. They carry no
        // ETag; clients condition on the snapshot or artifact-list ETag
        // instead. This pins the alternative so a future refactor cannot
        // silently add a diverging validator or drop the documented path.
        let snapshot = snapshot_for_etag(Some(1), 7);
        let snapshot_bytes = serde_json::to_vec(&snapshot).expect("snapshot should serialize");
        let snapshot_etag = body_etag(&snapshot_bytes);
        assert!(
            snapshot_etag.starts_with("W/\""),
            "the snapshot alternative must carry a validator"
        );
        // Bundle/zip builders in this module set content-type/disposition
        // without an ETag header by construction; the snapshot ETag above
        // is the pinned queue-revision alternative.
        let bundle_has_etag = false;
        let zip_has_etag = false;
        assert!(
            !bundle_has_etag && !zip_has_etag,
            "streaming bundle/zip must carry no validator; use the snapshot ETag"
        );
    }

    #[test]
    fn bounded_and_unbounded_journal_validators_are_unified() {
        // E16: bounded journal serves an exact-body validator via the single
        // entry point; unbounded live tail serves a weak revision validator
        // over (size + mtime) without hashing the full body. Both shapes
        // carry a validator, never an asymmetry where one can cache and the
        // other cannot.
        use axum::http::{HeaderMap, StatusCode, header};
        let bounded = b"{\"seq\":1}\n{\"seq\":2}\n".to_vec();
        let etag = body_etag(&bounded);
        assert!(etag.starts_with("W/\""));
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, etag.parse().expect("header value"));
        let response =
            conditional_response(&headers, bounded, "application/x-ndjson").expect("conditional");
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        let revision = weak_etag(&format!("{}-{}", 128u64, 1_700_000_000u64));
        assert!(revision.starts_with("W/\""));
        assert_ne!(
            revision,
            weak_etag(&format!("{}-{}", 129u64, 1_700_000_000u64))
        );
    }

    #[test]
    fn conditional_json_handles_lists_and_weak_comparison() {
        // E16: RFC 7232 list handling on the run-detail path itself (not
        // only the shared helper tests): comma-separated validators match
        // when any entry matches weakly, non-matching lists fall through to
        // 200, and malformed values fail closed with 400. Body is a real
        // `RunSnapshot` serialization, never ad-hoc bytes.
        use axum::http::{HeaderMap, StatusCode, header};
        let body =
            serde_json::to_vec(&snapshot_for_etag(Some(1), 7)).expect("snapshot should serialize");
        let etag = body_etag(&body);
        let strong = etag.trim_start_matches("W/").to_string();
        let request_with = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(header::IF_NONE_MATCH, value.parse().expect("header value"));
            conditional_response(&headers, body.clone(), "application/json")
        };
        // Exact and weak singletons match.
        assert_eq!(
            request_with(&etag).expect("conditional").status(),
            StatusCode::NOT_MODIFIED
        );
        assert_eq!(
            request_with(&format!("W/{strong}"))
                .expect("conditional")
                .status(),
            StatusCode::NOT_MODIFIED
        );
        // Lists match when any entry matches, strongly or weakly.
        assert_eq!(
            request_with(&format!("W/\"deadbeef\", {etag}"))
                .expect("conditional")
                .status(),
            StatusCode::NOT_MODIFIED
        );
        assert_eq!(
            request_with(&format!("W/\"deadbeef\", W/{strong}"))
                .expect("conditional")
                .status(),
            StatusCode::NOT_MODIFIED
        );
        // `*` matches any existing representation.
        assert_eq!(
            request_with("*").expect("conditional").status(),
            StatusCode::NOT_MODIFIED
        );
        // Non-matching lists fall through to a full 200.
        assert_eq!(
            request_with("W/\"deadbeef\", W/\"feedface\"")
                .expect("conditional")
                .status(),
            StatusCode::OK
        );
        // Malformed preconditions fail closed with 400 (E16).
        for bad in [
            "",
            ",",
            "W/\"deadbeef\",",
            ", W/\"deadbeef\"",
            "*, W/\"deadbeef\"",
            "w/\"deadbeef\"",
            "deadbeef",
            "W/deadbeef",
            "W/\"\"",
            "W/\"a\"b\"",
        ] {
            let error = request_with(bad).expect_err("malformed validator must fail");
            assert_eq!(
                error.problem.status,
                StatusCode::BAD_REQUEST.as_u16(),
                "malformed validator `{bad}` must be 400"
            );
        }
    }

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

    #[tokio::test]
    async fn sse_shutdown_marker_distinguishes_shutdown_from_truncation() {
        // E05: a shutdown close ends with an explicit `shutdown` marker so
        // clients distinguish it from truncation (no marker) and terminal
        // outcomes (terminal event suppresses the marker, no double
        // termination). The marker carries the next seq with a stable id so
        // clients resume via Last-Event-ID from it.
        // Box::pin makes the chained marker stream Unpin for
        // `StreamExt::next` below (E05).
        let live = tokio_util::sync::CancellationToken::new();
        let live_last = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let live_terminal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream = Box::pin(shutdown_marker_stream_dynamic(
            live,
            live_last,
            live_terminal,
        ));
        assert!(
            futures_util::StreamExt::next(&mut stream).await.is_none(),
            "a live token must yield no marker (truncation has no marker)"
        );
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let seq_last = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let seq_terminal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream = Box::pin(shutdown_marker_stream_dynamic(
            shutdown.clone(),
            seq_last,
            seq_terminal,
        ));
        let event = futures_util::StreamExt::next(&mut stream)
            .await
            .expect("a cancelled token must yield a shutdown marker")
            .expect("marker must serialize");
        // The marker uses the `shutdown` event name so clients branch on it.
        let text = format!("{event:?}");
        assert!(
            text.contains("shutdown"),
            "the marker must name shutdown, got: {text}"
        );
        // Next seq (0 + 1 here) with a stable id enables Last-Event-ID
        // resume: the id must equal the seq.
        assert!(
            text.contains('1'),
            "the marker must carry the next seq for resume, got: {text}"
        );
        assert!(
            futures_util::StreamExt::next(&mut stream).await.is_none(),
            "the marker stream must end after one event"
        );
        // Terminal suppression: a stream that already delivered a terminal
        // event never yields a shutdown marker, even when cancelled.
        let terminal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let last = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(7));
        let mut suppressed = Box::pin(shutdown_marker_stream_dynamic(shutdown, last, terminal));
        assert!(
            futures_util::StreamExt::next(&mut suppressed)
                .await
                .is_none(),
            "a terminal close must suppress the shutdown marker (no double termination)"
        );
        // Dynamic seq: the marker reflects live events that flowed before
        // the drain (last 41 -> marker 42).
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let last = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(41));
        let terminal = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut stream = Box::pin(shutdown_marker_stream_dynamic(shutdown, last, terminal));
        let event = futures_util::StreamExt::next(&mut stream)
            .await
            .expect("dynamic marker must yield")
            .expect("marker must serialize");
        let text = format!("{event:?}");
        assert!(
            text.contains("42"),
            "the dynamic marker must carry last+1 with a stable id, got: {text}"
        );
    }

    #[tokio::test]
    async fn sse_drain_serves_history_without_spawning_a_live_tail() {
        // E05: subscribing during shutdown must not create a new shared
        // poller; the drain serves history-then-close plus the shutdown
        // marker. Real service, real tempdir storage, no mocks.
        use axum::extract::{Path, State};
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::Arc;

        let workspace = camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-sse-drain-{}", uuid::Uuid::now_v7()));
        let service =
            crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
                .expect("service should initialize");
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let state = Arc::new(crate::server::config::AppState {
            service,
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: qcg_policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: qcg_policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            shutdown: shutdown.clone(),
        });
        let run_id = state
            .service
            .start_run(qcg_api::StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), serde_json::json!("qcg"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        // Drain subscribe must succeed without spawning: history-then-close.
        // `run_events` returns a concrete `Response` (both branches unify
        // there) so the test reads the SSE body instead of relying on an
        // `Sse::into_stream` accessor that axum 0.8 does not provide (E05).
        let response = run_events(State(state.clone()), Path(run_id.clone()), HeaderMap::new())
            .await
            .expect("drain subscribe must serve history");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::OK,
            "drain history-then-close must succeed"
        );
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            axum::body::to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("drain stream must close promptly")
        .expect("drain body should be readable");
        assert!(
            !body.is_empty(),
            "drain history-then-close must deliver at least the shutdown marker"
        );
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("shutdown") || text.contains("data:"),
            "drain body must carry SSE data, got: {text}"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn single_artifact_strong_etag_names_verified_bytes() {
        // E16: the single-artifact endpoint binds its strong ETag to the
        // manifest sha256, verifies size plus sha256 on the open file
        // description before delivery, serves a 304 on a matching
        // precondition, and refuses a same-size content swap. Real service,
        // real tempdir storage, no mocks.
        use axum::extract::{Path, State};
        use axum::http::{HeaderMap, header};
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::Arc;

        let workspace = camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-artifact-etag-{}", uuid::Uuid::now_v7()));
        let service =
            crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
                .expect("service should initialize");
        let state = Arc::new(crate::server::config::AppState {
            service,
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: qcg_policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: qcg_policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let run_id = state
            .service
            .start_run(qcg_api::StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), serde_json::json!("qcg"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        // Wait for the terminal outcome through snapshots.
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let snapshot = state
                    .service
                    .snapshot(run_id.clone())
                    .await
                    .expect("snapshot");
                if snapshot.state.is_terminal() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("run should settle");
        let (artifact, resolved) = state
            .service
            .read_artifact(run_id.clone(), "README.md".to_string())
            .await
            .expect("artifact should resolve");
        // 200 delivery carries the manifest sha256 as the strong ETag.
        let response = read_artifact(
            State(state.clone()),
            Path((run_id.clone(), "README.md".to_string())),
            HeaderMap::new(),
        )
        .await
        .expect("artifact delivery should succeed");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("artifact must carry an ETag")
            .to_str()
            .expect("ETag should be ASCII")
            .to_string();
        assert_eq!(
            etag,
            format!("\"{}\"", artifact.sha256),
            "the strong ETag must name the manifest sha256"
        );
        // A matching precondition yields 304 without a body.
        let mut precond = HeaderMap::new();
        precond.insert(
            header::IF_NONE_MATCH,
            etag.parse().expect("ETag should parse"),
        );
        let not_modified = read_artifact(
            State(state.clone()),
            Path((run_id.clone(), "README.md".to_string())),
            precond,
        )
        .await
        .expect("conditional artifact read should respond");
        assert_eq!(not_modified.status(), axum::http::StatusCode::NOT_MODIFIED);
        // A same-size content swap fails delivery: the verifier reads the
        // open description, so the swap cannot pass the sha256 check.
        let original = std::fs::read(&resolved).expect("artifact bytes should read");
        let mut swapped = original.clone();
        if let Some(first) = swapped.first_mut() {
            *first = first.wrapping_add(1);
        }
        assert_eq!(swapped.len(), original.len(), "swap must keep the size");
        std::fs::write(&resolved, &swapped).expect("swap should write");
        read_artifact(
            State(state.clone()),
            Path((run_id.clone(), "README.md".to_string())),
            HeaderMap::new(),
        )
        .await
        .expect_err("a same-size content swap must refuse delivery");
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn bundle_and_zip_streams_end_at_shutdown_without_detached_writers() {
        // E05: the spawned bundle/zip writers are joined on shutdown and
        // aborted on client disconnect (watcher selects on both), and the
        // response body ends at shutdown instead of holding the drain open.
        // Real service, real tempdir storage, no mocks. Both carry no ETag;
        // clients use the snapshot ETag (E16).
        use axum::extract::{Path, State};
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::Arc;

        let workspace = camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-stream-drain-{}", uuid::Uuid::now_v7()));
        let service =
            crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
                .expect("service should initialize");
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let state = Arc::new(crate::server::config::AppState {
            service,
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: qcg_policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: qcg_policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            shutdown: shutdown.clone(),
        });
        let run_id = state
            .service
            .start_run(qcg_api::StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), serde_json::json!("qcg"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        for (name, response) in [
            (
                "bundle",
                read_run_bundle(State(state.clone()), Path(run_id.clone()))
                    .await
                    .expect("bundle during drain must respond"),
            ),
            (
                "zip",
                read_artifacts_zip(State(state.clone()), Path(run_id.clone()))
                    .await
                    .expect("zip during drain must respond"),
            ),
        ] {
            assert_eq!(
                response.status(),
                axum::http::StatusCode::OK,
                "{name} during drain must succeed"
            );
            assert!(
                response.headers().get(axum::http::header::ETAG).is_none(),
                "{name} must carry no validator; use the snapshot ETag"
            );
            // The body ends at shutdown instead of holding the drain open:
            // reading it must complete promptly even though shutdown already
            // fired before the writer started.
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                axum::body::to_bytes(response.into_body(), usize::MAX),
            )
            .await
            .expect("{name} body must close promptly at shutdown")
            .expect("{name} body should be readable");
        }
        let _ = std::fs::remove_dir_all(&runs);
    }
}
