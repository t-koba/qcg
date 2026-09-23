use anyhow::Result;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use qcg_api::{ForkRun, RunListQuery, RunListResponse, RunSnapshot, StartRun};
use std::sync::Arc;

use super::config::AppState;
use super::error::ApiHttpError;
use super::idempotency::{IdempotentCall, with_idempotency};
use super::run_detail::{conditional_response, json_response_with_etag};

pub(crate) async fn list_runs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RunListQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let limit = query.limit.unwrap_or(qcg_api::RUN_LIST_LIMIT_DEFAULT);
    if !(qcg_api::RUN_LIST_LIMIT_MIN..=qcg_api::RUN_LIST_LIMIT_MAX).contains(&limit) {
        return Err(ApiHttpError::bad_request_field(
            "limit",
            format!(
                "limit must be between {} and {}",
                qcg_api::RUN_LIST_LIMIT_MIN,
                qcg_api::RUN_LIST_LIMIT_MAX
            ),
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
    // Stable chronological order across generators comes from
    // list_run_items as (started_at, run_id); run_id alone embeds the
    // generator prefix and never sorts globally by time (C01).
    // Cursor is `started_at|run_id` for the new order. Bare run_id cursors
    // are rejected instead of silently misordered.
    let cursor_position = match query.cursor.as_deref() {
        None => None,
        Some(cursor) => match cursor.split_once('|') {
            Some((started_at, run_id)) => Some((started_at.to_string(), run_id.to_string())),
            None => {
                return Err(ApiHttpError::bad_request_field(
                    "cursor",
                    "cursor must have form `started_at|run_id`",
                ));
            }
        },
    };
    // A run with an unparseable started_at is journal corruption: gc fails
    // on the same condition, so listing must fail closed as well instead of
    // silently dropping the run from filtered results.
    if since.is_some() {
        for item in &items {
            if let Err(error) = chrono::DateTime::parse_from_rfc3339(&item.started_at) {
                return Err(ApiHttpError::internal(format!(
                    "run `{}` has invalid started_at: {error}",
                    item.run_id
                )));
            }
        }
    }
    // Descending order serves newest-first views: the same
    // `started_at|run_id` cursor compares flipped, and the page is cut
    // from the newest end so capped fetches never lose recent runs (B12).
    // Ascending keeps the original contract byte-for-byte.
    let descending = matches!(query.order, Some(qcg_api::RunListOrder::Desc));
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
            && cursor_position
                .as_ref()
                .is_none_or(|(cursor_started, cursor_id)| {
                    if descending {
                        (&item.started_at, &item.run_id) < (cursor_started, cursor_id)
                    } else {
                        (&item.started_at, &item.run_id) > (cursor_started, cursor_id)
                    }
                })
    });
    if descending {
        items.reverse();
    }
    let next_cursor = (items.len() > limit).then(|| {
        let last = &items[limit - 1];
        format!("{}|{}", last.started_at, last.run_id)
    });
    items.truncate(limit);
    // Weak ETag over the exact list body (E16): any item move changes the
    // validator, and `If-None-Match` (including `*` and weak comparison)
    // yields 304 via the single conditional entry point. Syntactically
    // invalid validators fail closed with 400 (E16).
    let body = serde_json::to_vec(&RunListResponse { items, next_cursor })
        .map_err(ApiHttpError::internal)?;
    conditional_response(&headers, body, "application/json")
}

pub(crate) async fn start_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<StartRun>,
) -> Result<Response, ApiHttpError> {
    let body = serde_json::to_vec(&req).map_err(ApiHttpError::internal)?;
    // Reserve the run id before the claim so a crash between run creation
    // and Ready commit retries into the same run.
    let reserved = state
        .service
        .reserve_start_run_id(&req.generator_id)
        .map_err(ApiHttpError::from_api)?;
    let service = state.service.clone();
    with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "start_run",
        target: "",
        body: &body,
        created: true,
        reserved_run_id: Some(reserved),
        // The reservation rides in the pending claim and the commit proves
        // `request run_id == reserved run_id` (E02). A missing reservation
        // fails closed instead of minting an unclaimed run.
        execute: |reserved| async move {
            let Some(reserved) = reserved else {
                return Err(qcg_api::ApiError::Internal {
                    detail: "idempotency reservation is missing; commit refused".into(),
                });
            };
            service.start_run_with_id(req, Some(reserved)).await
        },
    })
    .await
}

pub(crate) fn respond_with_snapshot(
    snapshot: RunSnapshot,
    created: bool,
) -> Result<Response, ApiHttpError> {
    if created {
        return created_run_response(snapshot);
    }
    // Mutation replays carry the same exact-body validator as the
    // conditional GET so a client can condition its next read on the
    // returned ETag instead of re-fetching blindly (E16).
    let body = serde_json::to_vec(&snapshot).map_err(ApiHttpError::internal)?;
    json_response_with_etag(StatusCode::OK, body, None)
}
pub(crate) async fn fork_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(source_id): Path<String>,
    Json(request): Json<ForkRun>,
) -> Result<Response, ApiHttpError> {
    let body = serde_json::to_vec(&request).map_err(ApiHttpError::internal)?;
    let reserved = state
        .service
        .reserve_fork_run_id(&source_id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let service = state.service.clone();
    let source_id_query = source_id.clone();
    with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "fork_run",
        target: &source_id,
        body: &body,
        created: true,
        reserved_run_id: Some(reserved),
        // Reservation check mirrors `start_run` (E02): a missing
        // reservation fails closed instead of forking unclaimed.
        execute: |reserved| async move {
            let Some(reserved) = reserved else {
                return Err(qcg_api::ApiError::Internal {
                    detail: "idempotency reservation is missing; commit refused".into(),
                });
            };
            service
                .fork_run_with_id(&source_id_query, request, Some(reserved))
                .await
        },
    })
    .await
}

pub(crate) fn created_run_response(snapshot: RunSnapshot) -> Result<Response, ApiHttpError> {
    let location = format!("/api/runs/{}", snapshot.run_id);
    let body = serde_json::to_vec(&snapshot).map_err(ApiHttpError::internal)?;
    json_response_with_etag(StatusCode::CREATED, body, Some(location))
}
