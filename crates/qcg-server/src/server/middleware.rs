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
    // Generator assets stay readable without a token even on an
    // authenticated instance: the bundled UI shell must load before a token
    // can be entered, and declared assets are static files, not secrets.
    // Every JSON API route (including runs, journals, and artifacts) keeps
    // requiring the bearer credential.
    let generator_asset = request.method() == Method::GET
        && path.starts_with("/api/generators/")
        && path.contains("/assets/");
    if state.api_token_digest.is_none()
        || matches!(
            path,
            "/healthz" | "/api/openapi.json" | "/api/mcp/oauth/callback"
        )
        || generator_asset
    {
        return next.run(request).await;
    }
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(sha256_bytes);
    let authorized = match (&supplied, &state.api_token_digest) {
        (Some(supplied), Some(expected)) => constant_time_digest_eq(supplied, expected),
        // Unreachable: an absent digest returns early above, and an absent
        // credential never authorizes. Fail closed without panicking (E04).
        _ => false,
    };
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

/// Cardinality bound on `qcg_generator_runs_total` series: only this many
/// generators (highest run counts first) are exported with a label, so a
/// chaotic generator id space cannot blow up scrape size.
const METRICS_GENERATOR_LIMIT: usize = 20;

pub(crate) async fn metrics(State(state): State<Arc<AppState>>) -> Result<Response, ApiHttpError> {
    let runs = state
        .service
        .list_run_items()
        .await
        .map_err(ApiHttpError::from_api)?;
    // Single fold over the one `list_run_items` scan: every metric below is
    // derived here, so a scrape performs no second scan and no extra I/O.
    let mut states = BTreeMap::<String, usize>::new();
    let mut generators = BTreeMap::<String, usize>::new();
    let mut active = 0_usize;
    let mut queued = 0_usize;
    let mut waiting = 0_usize;
    let mut confirming = 0_usize;
    for run in &runs {
        *states.entry(run.state.to_string()).or_default() += 1;
        *generators.entry(run.generator_id.clone()).or_default() += 1;
        match run.state {
            qcg_api::RunStatus::Queued => {
                queued += 1;
                active += 1;
            }
            qcg_api::RunStatus::Running => active += 1,
            qcg_api::RunStatus::Waiting => waiting += 1,
            qcg_api::RunStatus::Confirming => confirming += 1,
            _ => {}
        }
    }
    let generator_count = generators.len();
    let generator_runs = top_generator_runs(generators.into_iter().collect());
    let mut body = String::from(
        "# HELP qcg_runs_total Number of durable runs by state.\n# TYPE qcg_runs_total gauge\n",
    );
    for (status, count) in states {
        body.push_str(&format!("qcg_runs_total{{state=\"{status}\"}} {count}\n"));
    }
    body.push_str("# HELP qcg_runs_active Number of queued or running runs.\n");
    body.push_str("# TYPE qcg_runs_active gauge\n");
    body.push_str(&format!("qcg_runs_active {active}\n"));
    body.push_str("# HELP qcg_runs_queued Number of queued runs.\n");
    body.push_str("# TYPE qcg_runs_queued gauge\n");
    body.push_str(&format!("qcg_runs_queued {queued}\n"));
    body.push_str("# HELP qcg_runs_waiting Number of runs waiting for interactive input.\n");
    body.push_str("# TYPE qcg_runs_waiting gauge\n");
    body.push_str(&format!("qcg_runs_waiting {waiting}\n"));
    body.push_str("# HELP qcg_runs_confirming Number of runs waiting for confirmation.\n");
    body.push_str("# TYPE qcg_runs_confirming gauge\n");
    body.push_str(&format!("qcg_runs_confirming {confirming}\n"));
    body.push_str(
        "# HELP qcg_generators Number of distinct generators represented in the run inventory.\n",
    );
    body.push_str("# TYPE qcg_generators gauge\n");
    body.push_str(&format!("qcg_generators {generator_count}\n"));
    body.push_str(
        "# HELP qcg_generator_runs_total Number of durable runs per generator (top 20 by run count).\n",
    );
    body.push_str("# TYPE qcg_generator_runs_total gauge\n");
    for (generator, count) in generator_runs {
        body.push_str(&format!(
            "qcg_generator_runs_total{{generator=\"{}\"}} {count}\n",
            escape_prometheus_label(&generator)
        ));
    }
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

/// Orders generator run counts by count descending (ties by generator id
/// ascending, so the output is deterministic) and keeps the top series.
fn top_generator_runs(mut runs: Vec<(String, usize)>) -> Vec<(String, usize)> {
    runs.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    runs.truncate(METRICS_GENERATOR_LIMIT);
    runs
}

/// Escapes a Prometheus label value per the text exposition format: only
/// backslash, double quote, and newline have escape sequences.
fn escape_prometheus_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(character),
        }
    }
    escaped
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

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use qcg_service::{LocalQcgService, RunStoreMode};

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    const SLOW_MANIFEST: &str = r#"
[generator]
id = "slow"
name = "Slow"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "metrics test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#;

    #[tokio::test]
    async fn metrics_exposes_run_and_generator_series() {
        // The new series must be present in one scrape and derived from the
        // single run-list scan the endpoint already performs.
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-metrics-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(generators.join("slow")).expect("generator dir should create");
        std::fs::write(generators.join("slow/qcg.toml"), SLOW_MANIFEST)
            .expect("slow manifest should write");
        let state = Arc::new(AppState {
            service: LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                1,
                qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                qcg_service::ServiceDeploymentPolicy::default(),
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: Default::default(),
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
                generator_id: "slow".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("slow run should start");
        let response = metrics(State(Arc::clone(&state)))
            .await
            .expect("metrics should render");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body should read");
        let body = String::from_utf8(body.to_vec()).expect("metrics must be UTF-8");
        // The run is queued or running, never waiting or confirming.
        assert!(
            body.contains("qcg_runs_active 1\n"),
            "active runs must count the started run: {body}"
        );
        assert!(
            body.contains("qcg_runs_total{state=\"queued\"} 1\n")
                || body.contains("qcg_runs_total{state=\"running\"} 1\n"),
            "the started run must appear in the state fold: {body}"
        );
        assert!(
            body.contains("qcg_runs_waiting 0\n") && body.contains("qcg_runs_confirming 0\n"),
            "zero gauges must still be exported: {body}"
        );
        assert!(
            body.contains("qcg_generators 1\n"),
            "the generator count must be derived from the run inventory: {body}"
        );
        assert!(
            body.contains("qcg_generator_runs_total{generator=\"slow\"} 1\n"),
            "per-generator run totals must be exported: {body}"
        );
        let _ = state.service.cancel(run_id).await;
    }

    #[tokio::test]
    async fn generator_assets_are_readable_without_a_token() {
        use axum::Router;
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt as _;

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-auth-assets-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generator dir should create");
        let state = Arc::new(AppState {
            service: LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                1,
                qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                qcg_service::ServiceDeploymentPolicy::default(),
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: Default::default(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: qcg_policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: qcg_policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: Some(sha256_bytes("secret")),
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let app = Router::new()
            .route(
                "/api/generators/{id}/assets/{*path}",
                get(|| async { "asset" }),
            )
            .route("/api/runs", get(|| async { "runs" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                require_api_auth,
            ));
        let request = |uri: &str, authorization: Option<&str>| {
            let mut builder = axum::http::Request::builder().uri(uri);
            if let Some(value) = authorization {
                builder = builder.header(axum::http::header::AUTHORIZATION, value);
            }
            builder.body(Body::empty()).expect("request should build")
        };
        let asset = app
            .clone()
            .oneshot(request("/api/generators/demo/assets/ui/index.html", None))
            .await
            .expect("asset request should respond");
        assert_eq!(
            asset.status(),
            axum::http::StatusCode::OK,
            "the UI shell must load before a token can be entered"
        );
        let unauthenticated = app
            .clone()
            .oneshot(request("/api/runs", None))
            .await
            .expect("run request should respond");
        assert_eq!(
            unauthenticated.status(),
            axum::http::StatusCode::UNAUTHORIZED,
            "JSON APIs must keep requiring the credential"
        );
        let authenticated = app
            .oneshot(request("/api/runs", Some("Bearer secret")))
            .await
            .expect("authenticated request should respond");
        assert_eq!(authenticated.status(), axum::http::StatusCode::OK);
    }

    #[test]
    fn generator_series_are_bounded_and_labels_escaped() {
        let mut runs = Vec::new();
        for index in 0..25 {
            runs.push((format!("gen-{index:02}"), index));
        }
        let top = top_generator_runs(runs);
        assert_eq!(top.len(), METRICS_GENERATOR_LIMIT);
        assert_eq!(top[0], ("gen-24".to_string(), 24));
        assert_eq!(top[19], ("gen-05".to_string(), 5));
        // Ties order deterministically by generator id.
        assert_eq!(
            top_generator_runs(vec![("b".to_string(), 1), ("a".to_string(), 1)]),
            vec![("a".to_string(), 1), ("b".to_string(), 1)]
        );
        assert_eq!(escape_prometheus_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
