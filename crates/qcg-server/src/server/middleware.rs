use anyhow::Result;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;

use super::config::AppState;
use super::error::ApiHttpError;
use super::run_detail::unsafe_generator_asset_path;

pub(crate) fn sha256_bytes(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

pub(crate) async fn require_api_auth(
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

pub(crate) async fn metrics(State(state): State<Arc<AppState>>) -> Result<Response, ApiHttpError> {
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

pub(crate) fn constant_time_digest_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

pub(crate) fn loopback_oauth_origins(address: SocketAddr) -> BTreeSet<String> {
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

pub(crate) async fn reject_unsafe_generator_asset_path(
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

pub(crate) async fn security_headers_middleware(request: Request, next: Next) -> Response {
    let generator_asset = request.uri().path().starts_with("/api/generators/")
        && request.uri().path().contains("/assets/");
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            if generator_asset {
                "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src 'self' data: blob:; media-src blob:; frame-src blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
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

pub(crate) fn require_local_oauth_origin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), ApiHttpError> {
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
