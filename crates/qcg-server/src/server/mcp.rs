use anyhow::Result;
use axum::Json;
use axum::body::Body;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use qcg_api::{McpAuthorizationStart, McpServerList};
use std::sync::Arc;

use super::config::AppState;
use super::error::ApiHttpError;
use super::middleware::require_local_oauth_origin;

pub(crate) async fn list_mcp_servers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<McpServerList>, ApiHttpError> {
    state
        .service
        .list_mcp_servers()
        .await
        .map(Json)
        .map_err(ApiHttpError::internal)
}

pub(crate) async fn start_mcp_authorization(
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

pub(crate) async fn clear_mcp_authorization(
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

pub(crate) async fn cancel_pending_mcp_authorization(
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

pub(crate) async fn complete_mcp_authorization(
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
