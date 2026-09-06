use anyhow::Result;
use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use qcg_api::{ForkRun, RunListQuery, RunListResponse, RunSnapshot, StartRun};
use std::sync::Arc;

use super::config::AppState;
use super::error::ApiHttpError;
use super::idempotency::with_idempotency;

pub(crate) async fn list_runs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RunListQuery>,
) -> Result<Json<RunListResponse>, ApiHttpError> {
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

pub(crate) async fn start_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<StartRun>,
) -> Result<Response, ApiHttpError> {
    let body = serde_json::to_vec(&req).map_err(ApiHttpError::internal)?;
    with_idempotency(&state, &headers, "start_run", "", &body, true, || async {
        state.service.start_run(req).await
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
    serde_json::to_value(&snapshot)
        .map(|value| Json(value).into_response())
        .map_err(ApiHttpError::internal)
}
pub(crate) async fn fork_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(source_id): Path<String>,
    Json(request): Json<ForkRun>,
) -> Result<Response, ApiHttpError> {
    let body = serde_json::to_vec(&request).map_err(ApiHttpError::internal)?;
    with_idempotency(
        &state,
        &headers,
        "fork_run",
        &source_id,
        &body,
        true,
        || async { state.service.fork_run(&source_id, request).await },
    )
    .await
}

pub(crate) fn created_run_response(snapshot: RunSnapshot) -> Result<Response, ApiHttpError> {
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
