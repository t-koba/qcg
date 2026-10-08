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
            HeaderValue::from_static("Bearer realm=\"api\""),
        );
        response
    }
}

/// Deployment policy for `/metrics` generator-series cardinality.
/// Mechanism renders the series; this policy chooses the budget and which
/// generators survive truncation. The scrape stays bounded in every case:
/// at most `generator_limit` series are ever emitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsPolicy {
    /// Maximum `generator_runs_total` series per scrape.
    pub generator_limit: usize,
    /// Generator ids that survive truncation when present in the inventory.
    /// Sorted ascending and deduplicated at resolve time so selection is
    /// deterministic. Pinned ids present in the inventory are emitted first
    /// (up to the budget); the remainder of the budget is filled top by
    /// run count.
    pub pinned_generators: Vec<String>,
}

impl Default for MetricsPolicy {
    fn default() -> Self {
        Self {
            generator_limit: policy::DEFAULT_METRICS_GENERATOR_LIMIT,
            pinned_generators: Vec::new(),
        }
    }
}

/// Resolves the metrics cardinality policy from the environment, once per
/// boot. `METRICS_GENERATOR_LIMIT` sets the budget (default 20, bounded
/// 1..=256 so the scrape cannot grow without bound); `METRICS_PINNED_GENERATORS`
/// is an optional comma-separated list of generator ids that survive
/// truncation when present (at most 64 ids, each 1..=256 bytes, no empty
/// entries). Every invalid value refuses boot instead of degrading
/// silently (E04).
pub(crate) fn resolve_metrics_policy() -> Result<MetricsPolicy, String> {
    let generator_limit = match std::env::var("METRICS_GENERATOR_LIMIT") {
        Err(_) => policy::DEFAULT_METRICS_GENERATOR_LIMIT,
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed)
                if (policy::MIN_METRICS_GENERATOR_LIMIT..=policy::MAX_METRICS_GENERATOR_LIMIT)
                    .contains(&parsed) =>
            {
                parsed
            }
            _ => {
                return Err(format!(
                    "invalid METRICS_GENERATOR_LIMIT `{value}`: must be an integer between {} and {}",
                    policy::MIN_METRICS_GENERATOR_LIMIT,
                    policy::MAX_METRICS_GENERATOR_LIMIT
                ));
            }
        },
    };
    let pinned_generators = match std::env::var("METRICS_PINNED_GENERATORS") {
        Err(_) => Vec::new(),
        Ok(value) if value.trim().is_empty() => Vec::new(),
        Ok(value) => {
            let mut pinned = Vec::new();
            for entry in value.split(',') {
                let id = entry.trim().to_string();
                if id.is_empty() {
                    return Err(format!(
                        "invalid METRICS_PINNED_GENERATORS `{value}`: must be a comma-separated list with no empty entries"
                    ));
                }
                if id.len() > policy::MAX_METRICS_PINNED_ID_BYTES {
                    return Err(format!(
                        "invalid METRICS_PINNED_GENERATORS `{id}`: pinned id must be at most {} bytes",
                        policy::MAX_METRICS_PINNED_ID_BYTES
                    ));
                }
                if !pinned.contains(&id) {
                    pinned.push(id);
                }
            }
            if pinned.len() > policy::MAX_METRICS_PINNED_GENERATORS {
                return Err(format!(
                    "invalid METRICS_PINNED_GENERATORS `{value}`: must list at most {} generators",
                    policy::MAX_METRICS_PINNED_GENERATORS
                ));
            }
            pinned.sort();
            pinned
        }
    };
    Ok(MetricsPolicy {
        generator_limit,
        pinned_generators,
    })
}

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
            api::RunStatus::Queued => {
                queued += 1;
                active += 1;
            }
            api::RunStatus::Running => active += 1,
            api::RunStatus::Waiting => waiting += 1,
            api::RunStatus::Confirming => confirming += 1,
            _ => {}
        }
    }
    let generator_count = generators.len();
    let generator_runs =
        select_generator_runs(generators.into_iter().collect(), &state.metrics_policy);
    let mut body = String::from(
        "# HELP runs_total Number of durable runs by state.\n# TYPE runs_total gauge\n",
    );
    body.push_str("# TYPE otlp_export_failures_total counter\n");
    body.push_str(&format!(
        "otlp_export_failures_total {}\n",
        super::otlp::EXPORT_FAILURES.load(std::sync::atomic::Ordering::Relaxed)
    ));
    body.push_str("# TYPE otlp_rejected_spans_total counter\n");
    body.push_str(&format!(
        "otlp_rejected_spans_total {}\n",
        super::otlp::REJECTED_SPANS.load(std::sync::atomic::Ordering::Relaxed)
    ));
    for (status, count) in states {
        body.push_str(&format!("runs_total{{state=\"{status}\"}} {count}\n"));
    }
    body.push_str("# HELP runs_active Number of queued or running runs.\n");
    body.push_str("# TYPE runs_active gauge\n");
    body.push_str(&format!("runs_active {active}\n"));
    body.push_str("# HELP runs_queued Number of queued runs.\n");
    body.push_str("# TYPE runs_queued gauge\n");
    body.push_str(&format!("runs_queued {queued}\n"));
    body.push_str("# HELP runs_waiting Number of runs waiting for interactive input.\n");
    body.push_str("# TYPE runs_waiting gauge\n");
    body.push_str(&format!("runs_waiting {waiting}\n"));
    body.push_str("# HELP runs_confirming Number of runs waiting for confirmation.\n");
    body.push_str("# TYPE runs_confirming gauge\n");
    body.push_str(&format!("runs_confirming {confirming}\n"));
    // Mechanism fact only: in-flight preempted runs held in memory.
    // No threshold or alert lives here; policy stays outside.
    let preempted = state.service.preempted_inflight().await;
    body.push_str("# HELP runs_preempted Number of in-memory runs currently marked preempted.\n");
    body.push_str("# TYPE runs_preempted gauge\n");
    body.push_str(&format!("runs_preempted {preempted}\n"));
    body.push_str(
        "# HELP generators Number of distinct generators represented in the run inventory.\n",
    );
    body.push_str("# TYPE generators gauge\n");
    body.push_str(&format!("generators {generator_count}\n"));
    body.push_str(
        "# HELP generator_runs_total Number of durable runs per generator (bounded selection).\n",
    );
    body.push_str("# TYPE generator_runs_total gauge\n");
    for (generator, count) in generator_runs {
        body.push_str(&format!(
            "generator_runs_total{{generator=\"{}\"}} {count}\n",
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

/// Selects the generator series for one scrape under `policy`.
/// Pinned generators present in the inventory are emitted first (id
/// ascending, so the output is deterministic), up to the budget; the
/// remainder of the budget is filled top by run count (count descending,
/// ties by id ascending). The output never exceeds the budget, so the
/// scrape stays bounded no matter how large the generator id space grows.
fn select_generator_runs(
    runs: Vec<(String, usize)>,
    policy: &MetricsPolicy,
) -> Vec<(String, usize)> {
    use std::collections::BTreeMap;
    let counts: BTreeMap<String, usize> = runs.into_iter().collect();
    let mut selected = Vec::new();
    for pinned in &policy.pinned_generators {
        if selected.len() >= policy.generator_limit {
            break;
        }
        if let Some(count) = counts.get(pinned) {
            selected.push((pinned.clone(), *count));
        }
    }
    let mut rest: Vec<(String, usize)> = counts
        .into_iter()
        .filter(|(id, _)| !selected.iter().any(|(kept, _)| kept == id))
        .collect();
    rest.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let remaining = policy.generator_limit.saturating_sub(selected.len());
    rest.truncate(remaining);
    // Keep pinned-first ordering deterministic: pinned stay id-ascending at
    // the head, the filled remainder stays count-descending after them.
    selected.extend(rest);
    selected
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

/// Derives the allowed `Host` values (`host[:port]`) from loopback origins
/// (`scheme://host[:port]`). The server already freezes the origins at boot
/// from the bound address, so no new configuration enters here (E04).
pub(crate) fn allowed_hosts_from_origins(origins: &BTreeSet<String>) -> BTreeSet<String> {
    origins
        .iter()
        .filter_map(|origin| {
            origin
                .strip_prefix("http://")
                .or_else(|| origin.strip_prefix("https://"))
        })
        .map(str::to_string)
        .collect()
}

/// Rejects DNS-rebinding probes on a loopback listener: the `Host` header
/// must be one of the loopback hosts for the bound port, else 403.
/// Non-loopback binds pass through: the operator's reverse proxy owns the
/// `Host` check there. Runs before authentication so a spoofed host never
/// reaches credential timing (E04 fail closed, no new config).
pub(crate) async fn require_loopback_host(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiHttpError> {
    if state.oauth_origin.is_none() {
        return Ok(next.run(request).await);
    }
    let allowed = allowed_hosts_from_origins(&state.oauth_allowed_origins);
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    match host {
        Some(host) if allowed.contains(host) => Ok(next.run(request).await),
        _ => Err(ApiHttpError::forbidden(
            "request Host is not a loopback address for this server",
        )),
    }
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
    use service::{LocalService, RunStoreMode};

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
            .join(format!("metrics-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(generators.join("slow")).expect("generator dir should create");
        std::fs::write(generators.join("slow/qcg.toml"), SLOW_MANIFEST)
            .expect("slow manifest should write");
        let state = Arc::new(AppState {
            service: LocalService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                1,
                policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                service::ServiceDeploymentPolicy::default(),
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: Default::default(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: None,
            artifact_limits: service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            metrics_policy: MetricsPolicy::default(),
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let run_id = state
            .service
            .start_run(api::StartRun {
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
            body.contains("runs_active 1\n"),
            "active runs must count the started run: {body}"
        );
        assert!(
            body.contains("runs_total{state=\"queued\"} 1\n")
                || body.contains("runs_total{state=\"running\"} 1\n"),
            "the started run must appear in the state fold: {body}"
        );
        assert!(
            body.contains("runs_waiting 0\n") && body.contains("runs_confirming 0\n"),
            "zero gauges must still be exported: {body}"
        );
        assert!(
            body.contains("generators 1\n"),
            "the generator count must be derived from the run inventory: {body}"
        );
        assert!(
            body.contains("generator_runs_total{generator=\"slow\"} 1\n"),
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
            .join(format!("auth-assets-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generator dir should create");
        let state = Arc::new(AppState {
            service: LocalService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                1,
                policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                service::ServiceDeploymentPolicy::default(),
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: Default::default(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: Some(sha256_bytes("secret")),
            artifact_limits: service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            metrics_policy: MetricsPolicy::default(),
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
    fn generator_series_respect_nondefault_budget_and_deterministic_order() {
        // Nondefault budget: only the top two survive, ordered by count
        // descending with id tie-breaks.
        let policy = MetricsPolicy {
            generator_limit: 2,
            pinned_generators: Vec::new(),
        };
        let mut runs = Vec::new();
        for index in 0..5 {
            runs.push((format!("gen-{index:02}"), index));
        }
        let selected = select_generator_runs(runs, &policy);
        assert_eq!(
            selected,
            vec![("gen-04".to_string(), 4), ("gen-03".to_string(), 3),],
            "a nondefault budget must truncate top by count"
        );
        // Ties order deterministically by generator id.
        assert_eq!(
            select_generator_runs(
                vec![("b".to_string(), 1), ("a".to_string(), 1)],
                &MetricsPolicy::default(),
            ),
            vec![("a".to_string(), 1), ("b".to_string(), 1)]
        );
        // Default budget still bounds a chaotic id space.
        let mut many = Vec::new();
        for index in 0..25 {
            many.push((format!("gen-{index:02}"), index));
        }
        let top = select_generator_runs(many, &MetricsPolicy::default());
        assert_eq!(top.len(), policy::DEFAULT_METRICS_GENERATOR_LIMIT);
        assert_eq!(top[0], ("gen-24".to_string(), 24));
    }

    #[test]
    fn pinned_generators_survive_truncation_deterministically() {
        // A low-volume pinned generator survives even when outside the
        // top-by-count budget; pinned ids lead id-ascending.
        let policy = MetricsPolicy {
            generator_limit: 2,
            pinned_generators: vec!["pinned-low".to_string()],
        };
        let runs = vec![
            ("gen-high-a".to_string(), 10),
            ("gen-high-b".to_string(), 9),
            ("pinned-low".to_string(), 1),
        ];
        assert_eq!(
            select_generator_runs(runs, &policy),
            vec![
                ("pinned-low".to_string(), 1),
                ("gen-high-a".to_string(), 10),
            ],
            "the pinned low-volume series must survive truncation"
        );
        // Absent pinned ids never fabricate series, and the budget is
        // never exceeded even when pinned plus inventory overflow it.
        let overflow = MetricsPolicy {
            generator_limit: 1,
            pinned_generators: vec!["a".to_string(), "b".to_string()],
        };
        let runs = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 99),
        ];
        let selected = select_generator_runs(runs, &overflow);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0, "a");
        let missing = MetricsPolicy {
            generator_limit: 1,
            pinned_generators: vec!["absent".to_string()],
        };
        assert_eq!(
            select_generator_runs(vec![("c".to_string(), 5)], &missing),
            vec![("c".to_string(), 5)],
            "an absent pin must not fabricate a series"
        );
    }

    #[test]
    fn generator_labels_are_escaped() {
        assert_eq!(escape_prometheus_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn allowed_hosts_derive_from_loopback_origins() {
        let addr: SocketAddr = "127.0.0.1:8080"
            .parse()
            .expect("loopback addr should parse");
        let origins = loopback_oauth_origins(addr);
        let hosts = allowed_hosts_from_origins(&origins);
        assert!(
            hosts.contains("127.0.0.1:8080"),
            "bound host must be allowed: {hosts:?}"
        );
        assert!(
            hosts.contains("localhost:8080"),
            "localhost alias must be allowed: {hosts:?}"
        );
        assert!(
            !hosts.iter().any(|host| host.contains("://")),
            "scheme must be stripped: {hosts:?}"
        );
        assert!(allowed_hosts_from_origins(&BTreeSet::new()).is_empty());
    }

    #[tokio::test]
    async fn loopback_host_rejects_spoofed_host() {
        use axum::Router;
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt as _;

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("loopback-host-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generator dir should create");
        let addr: SocketAddr = "127.0.0.1:8080"
            .parse()
            .expect("loopback addr should parse");
        let state = Arc::new(AppState {
            service: LocalService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                1,
                policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                service::ServiceDeploymentPolicy::default(),
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: Some(format!("http://{addr}")),
            oauth_allowed_origins: loopback_oauth_origins(addr),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            idempotency_ttl: policy::IDEMPOTENCY_TTL,
            idempotency_max_entries: policy::IDEMPOTENCY_MAX_ENTRIES,
            api_token_digest: None,
            artifact_limits: service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
            metrics_policy: MetricsPolicy::default(),
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let app = Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                require_loopback_host,
            ));
        let request = |host: Option<&str>| {
            let mut builder = axum::http::Request::builder().uri("/healthz");
            if let Some(host) = host {
                builder = builder.header(axum::http::header::HOST, host);
            }
            builder.body(Body::empty()).expect("request should build")
        };
        let ok = app
            .clone()
            .oneshot(request(Some("127.0.0.1:8080")))
            .await
            .expect("allowed host should respond");
        assert_eq!(ok.status(), axum::http::StatusCode::OK);
        let alias = app
            .clone()
            .oneshot(request(Some("localhost:8080")))
            .await
            .expect("localhost alias should respond");
        assert_eq!(alias.status(), axum::http::StatusCode::OK);
        let spoofed = app
            .clone()
            .oneshot(request(Some("evil.com")))
            .await
            .expect("spoofed host should respond");
        assert_eq!(
            spoofed.status(),
            axum::http::StatusCode::FORBIDDEN,
            "DNS-rebinding Host must fail closed"
        );
        let missing = app
            .oneshot(request(None))
            .await
            .expect("missing host should respond");
        assert_eq!(missing.status(), axum::http::StatusCode::FORBIDDEN);
    }
}
