use anyhow::Result;
use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use qcg_api::GeneratorSummary;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use super::config::AppState;
use super::error::ApiHttpError;
use super::run_detail::{conditional_json, content_type_for_name, weak_etag};

pub(crate) async fn healthz(State(state): State<Arc<AppState>>) -> Json<Value> {
    // Effective request body limit: null means no mechanistic limit
    // (Axum's default is disabled); a number is the enforced bound.
    Json(json!({
        "ok": true,
        "max_request_bytes": state.max_request_bytes,
    }))
}

pub(crate) async fn openapi() -> Json<Value> {
    Json(qcg_api::openapi_document(env!("CARGO_PKG_VERSION")))
}

pub(crate) async fn list_generators(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<GeneratorSummary>>, ApiHttpError> {
    state
        .service
        .list_generators()
        .await
        .map(Json)
        .map_err(ApiHttpError::from_api)
}

pub(crate) async fn describe_generator(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiHttpError> {
    let detail = state
        .service
        .describe(&id)
        .await
        .map_err(ApiHttpError::from_api)?;
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&detail).map_err(ApiHttpError::internal)?,
    ));
    conditional_json(&headers, weak_etag(&format!("generator-{digest}")), &detail)
}

pub(crate) async fn read_generator_asset(
    State(state): State<Arc<AppState>>,
    Path((id, path)): Path<(String, String)>,
) -> Result<Response, ApiHttpError> {
    let bytes = state
        .service
        .read_generator_asset_with_limit(id, path.clone(), state.asset_limit)
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
