use super::*;
use api::{AnswerPayload, ForkRun, RunListQuery, StartRun};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use camino::Utf8PathBuf;
use serde_json::json;
use service::{LocalService, RunStoreMode};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use policy::{IDEMPOTENCY_HEADER, IDEMPOTENCY_MAX_ENTRIES};

/// Binds an ephemeral loopback port for a test server, failing the test
/// on any refusal. `TEST_SKIP_IF_NO_LOOPBACK=1` is the explicit
/// opt-in for environments that cannot bind loopback at all; it reports
/// the skip instead of letting a silent return read as a pass.
async fn bind_loopback() -> Option<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => Some(listener),
        Err(error) => {
            if std::env::var("TEST_SKIP_IF_NO_LOOPBACK").is_ok_and(|value| value == "1") {
                eprintln!("skipping test: loopback bind failed and opted in: {error}");
                return None;
            }
            panic!("listener should bind on loopback: {error}");
        }
    }
}

/// `LocalService` for a test that must admit exactly one active run, so
/// a queueing, cancel, or drain assertion observes the concurrency bound
/// instead of racing the default. Exclusive store mode, no explicit
/// providers file, and the default deployment policy match
/// `crate::test_service`; only the active-run slot differs.
fn single_run_service(generators_dir: Utf8PathBuf, runs_dir: Utf8PathBuf) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        None,
        1,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        service::ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize")
}

#[test]
fn conditional_json_star_matches_any_existing_snapshot() {
    // E16: `If-None-Match: *` reports 304 for an existing resource and
    // unknown validators fall through to 200 (documented in the
    // http-server guide alongside the snapshot validator). Single entry
    // point: no caller ETag arg, one hash total. Body is a real
    // `RunSnapshot`, never ad-hoc bytes, so queue-revision coverage
    // stays on the served shape.
    use crate::server::conditional_response;
    let snapshot = api::RunSnapshot {
        run_id: "run-star".to_string(),
        state: api::RunStatus::Queued,
        seq: 3,
        contract_sha256: None,
        generator_id: "gen".to_string(),
        artifacts: None,
        question: None,
        confirm: None,
        queued_at: None,
        queue_position: Some(2),
        priority: 0,
        parent_run_id: None,
        metrics: None,
        labels: BTreeMap::new(),
    };
    let body = serde_json::to_vec(&snapshot).expect("snapshot should serialize");
    let mut star = axum::http::HeaderMap::new();
    star.insert(
        axum::http::header::IF_NONE_MATCH,
        axum::http::HeaderValue::from_static("*"),
    );
    let response =
        conditional_response(&star, body.clone(), "application/json").expect("conditional");
    assert_eq!(response.status(), axum::http::StatusCode::NOT_MODIFIED);
    let mut wrong = axum::http::HeaderMap::new();
    wrong.insert(
        axum::http::header::IF_NONE_MATCH,
        axum::http::HeaderValue::from_static("W/\"deadbeef\""),
    );
    let response = conditional_response(&wrong, body, "application/json").expect("conditional");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}

#[tokio::test]
async fn snapshot_etag_reflects_queue_position_changes() {
    // E16: this run's seq/state do not change when a run ahead of it
    // settles, but its queue_position does; the validator is the exact
    // body digest, so the client receives the updated body.
    use tower::ServiceExt as _;

    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("etag-queue-{}", uuid::Uuid::now_v7()));
    let generators = root.join("generators");
    std::fs::create_dir_all(generators.join("slow")).expect("slow generator dir");
    std::fs::write(
            generators.join("slow/qcg.toml"),
            r#"
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
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "queue test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest");
    std::fs::create_dir_all(generators.join("fast")).expect("fast generator dir");
    std::fs::write(
        generators.join("fast/qcg.toml"),
        r#"
[generator]
id = "fast"
name = "Fast"
version = "0.1.0"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[flow]]
id = "write"
type = "write"
[flow.params]
output_file = "result.txt"
content = "done""#,
    )
    .expect("fast manifest");
    let runs = root.join("runs");
    let state = Arc::new(test_state(
        single_run_service(generators.clone(), runs.clone()),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let config = ServerConfig {
        generators_dir: generators.clone(),
        providers_path: None,
        extra_generators_dirs: Vec::new(),
        runs_dir: runs.clone(),
        max_active_runs: 1,
        max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
        run_store_mode: RunStoreMode::Exclusive,
        cors_origins: Vec::new(),
        api_token: None,
        max_request_bytes: None,
        max_artifact_bytes: None,
        max_artifact_entries: None,
        max_asset_bytes: None,
        max_total_steps: None,
    };
    let validated_cors =
        super::server::parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
    let app = build_router(&state, &config, &validated_cors, None).expect("router should build");
    let start = |generator: &str| StartRun {
        generator_id: generator.into(),
        inputs: BTreeMap::new(),
        ..Default::default()
    };
    let slow = state
        .service
        .start_run(start("slow"))
        .await
        .expect("slow run should start");
    // A queued front run ahead of the target: with one slot held by the
    // 30 s slow run, both front and target stay queued, so the target
    // stably observes position 2 until the front settles (E16).
    let front = state
        .service
        .start_run(start("fast"))
        .await
        .expect("front run should start");
    let target = state
        .service
        .start_run(start("fast"))
        .await
        .expect("target run should start");
    // Wait until the slow run holds the single slot: from then on the
    // queued order is deterministically front(1), target(2).
    for _ in 0..400 {
        if state
            .service
            .snapshot(slow.clone())
            .await
            .expect("snapshot")
            .state
            == api::RunStatus::Running
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut target_position = None;
    for _ in 0..400 {
        target_position = state
            .service
            .snapshot(target.clone())
            .await
            .expect("snapshot")
            .queue_position;
        if target_position == Some(2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        target_position,
        Some(2),
        "target must queue behind the front run"
    );
    let get = |etag: Option<&str>| {
        let app = app.clone();
        let target = target.clone();
        let etag = etag.map(str::to_string);
        async move {
            let mut request = axum::http::Request::builder()
                .uri(format!("/api/runs/{target}"))
                .body(axum::body::Body::empty())
                .expect("request should build");
            if let Some(etag) = etag {
                request.headers_mut().insert(
                    header::IF_NONE_MATCH,
                    HeaderValue::from_str(&etag).expect("test etag should be a valid header value"),
                );
            }
            app.oneshot(request).await.expect("snapshot request")
        }
    };
    // Capture the etag and body atomically from one response: the
    // front run still holds position 1 ahead, so position 2 is stable
    // here (E16).
    let etag = {
        let response = get(None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .expect("etag should be present")
            .to_string();
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("snapshot should be JSON");
        assert_eq!(
            body["queue_position"],
            json!(2),
            "HTTP body must carry the observed queue position"
        );
        etag
    };
    // Settle the front run without touching the target's journal.
    state
        .service
        .cancel(front.clone())
        .await
        .expect("front cancel");
    for _ in 0..400 {
        if state
            .service
            .snapshot(front.clone())
            .await
            .expect("snapshot")
            .state
            .is_terminal()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut moved = false;
    for _ in 0..400 {
        if state
            .service
            .snapshot(target.clone())
            .await
            .expect("snapshot")
            .queue_position
            == Some(1)
        {
            moved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(moved, "target must advance to position 1 while queued");
    let response = get(Some(&etag)).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a changed queue position must return the updated body, not 304"
    );
    let updated_etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("updated etag should be present")
        .to_string();
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body"),
    )
    .expect("snapshot should be JSON");
    assert_eq!(body["queue_position"], json!(1));
    assert_ne!(
        updated_etag, etag,
        "the updated body must carry a new validator"
    );
    // An unchanged (terminal) snapshot still uses 304. Let the target
    // run after the blocker is canceled so its terminal metrics are
    // stable.
    let _ = state.service.cancel(slow).await;
    for _ in 0..400 {
        if state
            .service
            .snapshot(target.clone())
            .await
            .expect("snapshot")
            .state
            .is_terminal()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let response = get(None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let terminal_etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("terminal etag should be present")
        .to_string();
    let response = get(Some(&terminal_etag)).await;
    assert_eq!(
        response.status(),
        StatusCode::NOT_MODIFIED,
        "an unchanged snapshot must reuse the conditional response"
    );
    // The artifacts endpoint uses the same exact-body validator: a
    // repeated conditional request is a 304, not a stale artifact list.
    let artifacts = |etag: Option<String>| {
        let app = app.clone();
        let target = target.clone();
        async move {
            let mut request = axum::http::Request::builder()
                .uri(format!("/api/runs/{target}/artifacts"))
                .body(axum::body::Body::empty())
                .expect("request should build");
            if let Some(etag) = etag {
                request.headers_mut().insert(
                    header::IF_NONE_MATCH,
                    HeaderValue::from_str(&etag).expect("test etag should be a valid header value"),
                );
            }
            app.oneshot(request).await.expect("artifacts request")
        }
    };
    let response = artifacts(None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let artifacts_etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("artifacts etag should be present")
        .to_string();
    let response = artifacts(Some(artifacts_etag)).await;
    assert_eq!(
        response.status(),
        StatusCode::NOT_MODIFIED,
        "an unchanged artifact body must reuse the conditional response"
    );
    // Cancel any remaining runs so the test releases its store lock.
    let _ = state.service.cancel(target).await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn snapshot_etag_tracks_live_metrics_across_seq() {
    // E16: live metrics advance without a journal move, so an unchanged
    // validator must never serve them as 304: the client always gets a
    // fresh body.
    use tower::ServiceExt as _;

    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("etag-live-{}", uuid::Uuid::now_v7()));
    let generators = root.join("generators");
    std::fs::create_dir_all(generators.join("slow")).expect("slow generator dir");
    std::fs::write(
            generators.join("slow/qcg.toml"),
            r#"
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
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest");
    let runs = root.join("runs");
    let state = Arc::new(test_state(
        single_run_service(generators.clone(), runs.clone()),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let config = ServerConfig {
        generators_dir: generators.clone(),
        providers_path: None,
        extra_generators_dirs: Vec::new(),
        runs_dir: runs.clone(),
        max_active_runs: 1,
        max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
        run_store_mode: RunStoreMode::Exclusive,
        cors_origins: Vec::new(),
        api_token: None,
        max_request_bytes: None,
        max_artifact_bytes: None,
        max_artifact_entries: None,
        max_asset_bytes: None,
        max_total_steps: None,
    };
    let validated_cors =
        super::server::parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
    let app = build_router(&state, &config, &validated_cors, None).expect("router should build");
    let id = state
        .service
        .start_run(StartRun {
            generator_id: "slow".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("slow run should start");
    let get = |etag: Option<&str>| {
        let app = app.clone();
        let id = id.clone();
        let etag = etag.map(str::to_string);
        async move {
            let mut request = axum::http::Request::builder()
                .uri(format!("/api/runs/{id}"))
                .body(axum::body::Body::empty())
                .expect("request should build");
            if let Some(etag) = etag {
                request.headers_mut().insert(
                    header::IF_NONE_MATCH,
                    HeaderValue::from_str(&etag).expect("test etag should be a valid header value"),
                );
            }
            app.oneshot(request).await.expect("snapshot request")
        }
    };
    let response = get(None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("etag should be present")
        .to_string();
    // Let live metrics advance while the journal stays put. Durations
    // quantize to seconds for stable ETags (E16), so sleep past a
    // second boundary to observe fresh metrics.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let response = get(Some(&etag)).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "advanced live metrics must return a fresh body, not 304"
    );
    let _ = state.service.cancel(id).await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn artifacts_etag_changes_without_a_sequence_move() {
    // E16: the artifact manifest can change while the journal stays
    // put; the validator must follow the body and return 200 rather
    // than a stale 304.
    use tower::ServiceExt as _;

    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("etag-artifacts-{}", uuid::Uuid::now_v7()));
    let generators = root.join("generators");
    std::fs::create_dir_all(generators.join("slow")).expect("slow generator dir");
    std::fs::write(
            generators.join("slow/qcg.toml"),
            r#"
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
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "artifacts test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest");
    let runs = root.join("runs");
    let state = Arc::new(test_state(
        single_run_service(generators.clone(), runs.clone()),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let config = ServerConfig {
        generators_dir: generators.clone(),
        providers_path: None,
        extra_generators_dirs: Vec::new(),
        runs_dir: runs.clone(),
        max_active_runs: 1,
        max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
        run_store_mode: RunStoreMode::Exclusive,
        cors_origins: Vec::new(),
        api_token: None,
        max_request_bytes: None,
        max_artifact_bytes: None,
        max_artifact_entries: None,
        max_asset_bytes: None,
        max_total_steps: None,
    };
    let validated_cors =
        super::server::parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
    let app = build_router(&state, &config, &validated_cors, None).expect("router should build");
    let id = state
        .service
        .start_run(StartRun {
            generator_id: "slow".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("slow run should start");
    let manifest = |label: &str| {
        serde_json::json!({
            "artifacts": [{
                "path": "reports/result.txt",
                "sha256": "abc",
                "bytes": 3,
                "label": label,
                "required": true,
            }],
        })
        .to_string()
    };
    let run_dir = state
        .service
        .run_dir_for(id.as_str())
        .await
        .expect("run dir");
    let meta = service::run_meta_dir(&run_dir);
    std::fs::write(meta.join("outputs.json"), manifest("v1")).expect("manifest v1");
    let get = |etag: Option<&str>| {
        let app = app.clone();
        let id = id.clone();
        let etag = etag.map(str::to_string);
        async move {
            let mut request = axum::http::Request::builder()
                .uri(format!("/api/runs/{id}/artifacts"))
                .body(axum::body::Body::empty())
                .expect("request should build");
            if let Some(etag) = etag {
                request.headers_mut().insert(
                    header::IF_NONE_MATCH,
                    HeaderValue::from_str(&etag).expect("test etag should be a valid header value"),
                );
            }
            app.oneshot(request).await.expect("artifacts request")
        }
    };
    let response = get(None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("etag should be present")
        .to_string();
    std::fs::write(meta.join("outputs.json"), manifest("v2")).expect("manifest v2");
    let response = get(Some(&etag)).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a changed artifact body must return 200, not a stale 304"
    );
    let _ = state.service.cancel(id).await;
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn shutdown_rejects_mutating_requests_and_closes_sse() {
    // E05: after the shutdown token is set, new mutating requests are
    // refused and SSE streams end instead of holding the drain open.
    use tower::ServiceExt as _;

    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("shutdown-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let config = ServerConfig {
        generators_dir: workspace.join("fixtures/generators"),
        providers_path: None,
        extra_generators_dirs: Vec::new(),
        runs_dir: runs.clone(),
        max_active_runs: policy::DEFAULT_MAX_ACTIVE_RUNS,
        max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
        run_store_mode: RunStoreMode::Exclusive,
        cors_origins: Vec::new(),
        api_token: None,
        max_request_bytes: None,
        max_artifact_bytes: None,
        max_artifact_entries: None,
        max_asset_bytes: None,
        max_total_steps: None,
    };
    let validated_cors =
        super::server::parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
    let app = build_router(&state, &config, &validated_cors, None).expect("router should build");
    let waiting = state
        .service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    for _ in 0..400 {
        if state
            .service
            .snapshot(waiting.clone())
            .await
            .expect("snapshot")
            .state
            == api::RunStatus::Waiting
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    state.shutdown.cancel();
    // The SSE stream ends even though the run is not terminal.
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/runs/{waiting}/events"))
                .body(axum::body::Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("events request");
    assert_eq!(response.status(), StatusCode::OK);
    let _body = tokio::time::timeout(
        Duration::from_secs(2),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("a cancelled shutdown token must close the SSE stream")
    .expect("stream body should be readable");
    // New mutating work is refused.
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::POST)
                .uri("/api/runs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(
                    json!({ "generator_id": "hello-template", "inputs": { "name": "test" } })
                        .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("start request");
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "mutating requests must be refused during shutdown"
    );
    let _ = state.service.cancel(waiting).await;
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn invalid_policy_refuses_boot_before_recovery_runs() {
    // E04: deployment policy is resolved before any recovery task
    // starts, so an invalid budget refuses boot with zero recovered
    // side effects and releases the run-store lock.
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("boot-order-{}", uuid::Uuid::now_v7()));
    let generators = root.join("generators");
    std::fs::create_dir_all(generators.join("recovered")).expect("generator dir");
    std::fs::write(
            generators.join("recovered/qcg.toml"),
            r#"
[generator]
id = "recovered"
name = "Recovered"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "echo ran > marker.txt"], purpose = "recovery probe", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "mark"
type = "command"
[flow.params]
command = ["sh", "-c", "echo ran > marker.txt"]

[[flow]]
id = "ask"
type = "ask_user"
[flow.params]
content = "Continue?"
options = ["yes"]"#,
        )
        .expect("recovered manifest");
    let runs = root.join("runs");
    // Produce a real admission journal with a valid contract digest,
    // then suspend it and truncate back to the admission record so
    // recovery would replay from the beginning (writing `marker.txt`).
    let service = crate::test_service(generators.clone(), runs.clone(), None)
        .expect("seeding service should initialize");
    let run_id = service
        .start_run(StartRun {
            generator_id: "recovered".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("seeding run should start");
    for _ in 0..400 {
        if state_is_waiting(&service, &run_id).await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let run_dir = runs.join(&run_id);
    let journal_path = run_dir.join("meta/journal.jsonl");
    let admission: String = std::fs::read_to_string(&journal_path)
        .expect("journal should be readable")
        .lines()
        .filter(|line| line.contains("\"t\":\"run_queued\""))
        .map(|line| format!("{line}\n"))
        .collect();
    drop(service);
    std::fs::write(&journal_path, admission).expect("truncated journal should be written");
    let marker = run_dir.join("output/marker.txt");
    let _ = std::fs::remove_file(&marker);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let error = serve_with_listener(
        ServerConfig {
            generators_dir: generators.clone(),
            providers_path: None,
            extra_generators_dirs: Vec::new(),
            runs_dir: runs.clone(),
            max_active_runs: policy::DEFAULT_MAX_ACTIVE_RUNS,
            max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
            run_store_mode: RunStoreMode::Exclusive,
            cors_origins: Vec::new(),
            api_token: None,
            max_request_bytes: None,
            max_artifact_bytes: None,
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: Some(0),
        },
        listener,
    )
    .await
    .expect_err("an invalid step budget must refuse boot");
    assert!(
        error.to_string().contains("step budget"),
        "refusal must name the invalid policy: {error}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !marker.exists(),
        "no recovered run may execute before policy validation"
    );
    let service = crate::test_service(generators, runs.clone(), None)
        .expect("the run-store lock must be released after a refused boot");
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

async fn state_is_waiting(service: &LocalService, id: &str) -> bool {
    service
        .snapshot(id.to_string())
        .await
        .is_ok_and(|snapshot| snapshot.state == api::RunStatus::Waiting)
}

#[tokio::test]
async fn router_implements_every_documented_route() {
    use tower::ServiceExt as _;

    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("router-routes-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let config = ServerConfig {
        generators_dir: workspace.join("fixtures/generators"),
        providers_path: None,
        extra_generators_dirs: Vec::new(),
        runs_dir: runs.clone(),
        max_active_runs: policy::DEFAULT_MAX_ACTIVE_RUNS,
        max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
        run_store_mode: RunStoreMode::Exclusive,
        cors_origins: Vec::new(),
        api_token: None,
        max_request_bytes: None,
        max_artifact_bytes: None,
        max_artifact_entries: None,
        max_asset_bytes: None,
        max_total_steps: None,
    };
    let validated_cors =
        super::server::parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
    let app = super::server::build_router(&state, &config, &validated_cors, None)
        .expect("router should build");

    // The OpenAPI table is the single canon: every entry must be well-formed
    // and unique so generated clients and docs cannot drift.
    let mut seen = BTreeSet::new();
    for route in api::API_ROUTES {
        assert!(!route.method.is_empty() && !route.path.is_empty());
        assert!(route.path.starts_with('/'), "{}", route.path);
        assert!(
            seen.insert((route.method, route.path)),
            "duplicate route {} {}",
            route.method,
            route.path
        );
    }

    // Every documented path must be routed. PATCH is unused by all routes,
    // so a routed path answers 405 while an unrouted one falls through to
    // the 404 fallback without invoking any handler.
    for route in api::API_ROUTES {
        let mut path = String::new();
        let mut rest = route.path;
        while let Some(start) = rest.find('{') {
            path.push_str(&rest[..start]);
            let end = rest.find('}').expect("route parameter must close");
            path.push('x');
            rest = &rest[end + 1..];
        }
        path.push_str(rest);
        let request = axum::http::Request::builder()
            .method(axum::http::Method::PATCH)
            .uri(path.clone())
            .body(axum::body::Body::empty())
            .expect("probe request should build");
        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("probe request should route");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::METHOD_NOT_ALLOWED,
            "{path} is not routed"
        );
    }
    let _ = std::fs::remove_dir_all(runs);
}

#[test]
fn oauth_accepts_only_loopback_aliases_on_the_bound_port() {
    let origins = loopback_oauth_origins("127.0.0.1:43123".parse().expect("valid address"));
    assert!(origins.contains("http://127.0.0.1:43123"));
    assert!(origins.contains("http://localhost:43123"));
    assert!(origins.contains("http://[::1]:43123"));
    assert!(!origins.contains("http://localhost:43124"));

    let remote = loopback_oauth_origins("192.0.2.1:43123".parse().expect("valid address"));
    assert!(remote.is_empty());
}

#[tokio::test]
async fn explicit_providers_path_is_authoritative() {
    let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory should be UTF-8")
        .join(format!("server-providers-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let Some(listener) = bind_loopback().await else {
        return;
    };
    let missing = root.join("providers.toml");
    let error = super::serve_with_listener(
        ServerConfig {
            generators_dir: root.join("generators"),
            providers_path: Some(missing.clone()),
            extra_generators_dirs: Vec::new(),
            runs_dir: root.join("runs"),
            max_active_runs: policy::DEFAULT_MAX_ACTIVE_RUNS,
            max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
            run_store_mode: RunStoreMode::Exclusive,
            cors_origins: Vec::new(),
            api_token: None,
            max_request_bytes: None,
            max_artifact_bytes: None,
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: None,
        },
        listener,
    )
    .await
    .expect_err("an explicit missing path must not fall back to another registry");
    assert!(error.to_string().contains(missing.as_str()));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn concurrent_idempotent_start_creates_one_run() {
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("idempotency-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("same-run"));
    let request = StartRun {
        generator_id: "hello-template".into(),
        inputs: BTreeMap::from([("name".into(), json!("test"))]),
        ..Default::default()
    };

    let (first, second) = tokio::join!(
        start_run(
            State(Arc::clone(&state)),
            headers.clone(),
            Json(request.clone())
        ),
        start_run(State(Arc::clone(&state)), headers, Json(request))
    );
    let first = first.expect("first request should start");
    let second = second.expect("second request should reuse the run");
    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(second.status(), StatusCode::CREATED);
    assert_eq!(
        first.headers().get(header::LOCATION),
        second.headers().get(header::LOCATION)
    );
    assert_eq!(
        state
            .service
            .list_run_items()
            .await
            .expect("runs should be listable")
            .len(),
        1
    );

    let invalid_since = list_runs(
        State(Arc::clone(&state)),
        Query(RunListQuery {
            since: Some("not-a-date".into()),
            ..RunListQuery::default()
        }),
        HeaderMap::new(),
    )
    .await
    .expect_err("invalid since must be rejected");
    assert_eq!(
        invalid_since.problem.status,
        StatusCode::BAD_REQUEST.as_u16()
    );
    assert_eq!(invalid_since.problem.errors[0].field, "since");

    {
        let mut entries = state.idempotency.lock().await;
        entries.clear();
        for index in 0..IDEMPOTENCY_MAX_ENTRIES {
            let (completed, _) = tokio::sync::watch::channel(false);
            entries.insert(
                format!("pending-{index}"),
                IdempotencyEntry::Pending {
                    digest: "digest".into(),
                    owner_id: uuid::Uuid::now_v7(),
                    created_at: Instant::now(),
                    completed,
                },
            );
        }
    }
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("at-capacity"));
    let capacity_request = StartRun {
        generator_id: "hello-template".into(),
        inputs: BTreeMap::from([("name".into(), json!("test"))]),
        ..Default::default()
    };
    let error = start_run(
        State(Arc::clone(&state)),
        headers,
        Json(capacity_request.clone()),
    )
    .await
    .expect_err("pending entries must not be evicted at capacity");
    assert_eq!(
        error.problem.status,
        StatusCode::SERVICE_UNAVAILABLE.as_u16()
    );
    assert!(
        state
            .idempotency
            .lock()
            .await
            .values()
            .all(|entry| matches!(entry, IdempotencyEntry::Pending { .. }))
    );
    // E02: capacity-overflow 503 keeps the durable pending claim so a
    // retry rejoins the same claim instead of minting a new one. Memory
    // alone cannot prove it: assert the concrete durable artifact.
    assert!(
        !state.idempotency.lock().await.contains_key("at-capacity"),
        "the 503 must not publish a memory entry for the rejected key"
    );
    {
        use sha2::Digest as _;
        let pending_path = runs.join("idempotency").join(format!(
            "{}.pending.json",
            hex::encode(sha2::Sha256::digest("at-capacity".as_bytes())),
        ));
        assert!(
            pending_path.as_std_path().exists(),
            "the durable pending claim file must persist across the 503"
        );
        let bytes = std::fs::read(pending_path.as_std_path())
            .expect("pending claim file should be readable");
        assert!(
            !bytes.is_empty(),
            "the surviving pending claim must not be empty"
        );
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).expect("pending claim should parse");
        assert_eq!(
            value.get("key").and_then(serde_json::Value::as_str),
            Some("at-capacity"),
            "the surviving claim must belong to the rejected key"
        );
        // Re-claim with the same request digest must observe the live
        // claim as Peer (not Absent/Owner): the retry rejoins the kept
        // claim instead of starting a fresh chain.
        let body = serde_json::to_vec(&capacity_request).expect("request should serialize");
        let mut digest_input = Vec::with_capacity("start_run".len() + body.len() + 2);
        digest_input.extend_from_slice(b"start_run");
        digest_input.push(0);
        digest_input.push(0);
        digest_input.extend_from_slice(&body);
        let digest = hex::encode(sha2::Sha256::digest(&digest_input));
        match crate::server::claim_durable_pending(
            &runs,
            "at-capacity",
            &digest,
            None,
            policy::IDEMPOTENCY_TTL,
        )
        .expect("re-claim should not error")
        {
            crate::server::ClaimOutcome::Peer => {}
            other => panic!("the kept claim must re-claim as peer, got {other:?}"),
        }
    }
}

/// Removes the temp runs dir on drop so a failed assertion cannot leak
/// test directories (E04).
struct IdempotencyTempGuard(Utf8PathBuf);
impl Drop for IdempotencyTempGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.as_std_path());
    }
}

#[tokio::test]
async fn idempotent_conflict_starts_no_second_run() {
    // E02: reusing a key with different content must conflict before
    // any second execution starts — through the memory path and,
    // after clearing memory, through the durable path.
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("idempotency-conflict-{}", uuid::Uuid::now_v7()));
    let _temp_guard = IdempotencyTempGuard(runs.clone());
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let request_for = |name: &str| StartRun {
        generator_id: "hello-template".into(),
        inputs: BTreeMap::from([("name".into(), json!(name))]),
        ..Default::default()
    };
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("conflict-run"));
    start_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Json(request_for("test")),
    )
    .await
    .expect("first request should start");
    assert_eq!(
        state
            .service
            .list_run_items()
            .await
            .expect("listable")
            .len(),
        1
    );
    for (label, clear_memory) in [("memory", false), ("durable", true)] {
        if clear_memory {
            // Simulate a restart for the durable path: same runs dir,
            // empty in-memory map.
            state.idempotency.lock().await.clear();
        }
        let error = start_run(
            State(Arc::clone(&state)),
            headers.clone(),
            Json(request_for("someone-else")),
        )
        .await
        .expect_err(&format!("{label} path must conflict on different content"));
        assert_eq!(
            error.problem.status,
            StatusCode::CONFLICT.as_u16(),
            "{label} path must report 409"
        );
        assert_eq!(
            state
                .service
                .list_run_items()
                .await
                .expect("listable")
                .len(),
            1,
            "{label} path must not have started a second run"
        );
    }
}

#[tokio::test]
async fn idempotent_start_reuses_run_after_restart() {
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("idempotency-restart-{}", uuid::Uuid::now_v7()));
    let make_state = || {
        Arc::new(test_state(
            crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
                .expect("service should initialize"),
            runs.clone(),
            None,
            tokio_util::sync::CancellationToken::new(),
        ))
    };
    let state = make_state();
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("restart-run"));
    let request = StartRun {
        generator_id: "hello-template".into(),
        inputs: BTreeMap::from([("name".into(), json!("test"))]),
        ..Default::default()
    };
    let first = start_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Json(request.clone()),
    )
    .await
    .expect("first request should start");
    let first_location = first
        .headers()
        .get(header::LOCATION)
        .expect("start should carry a location")
        .clone();
    drop(state);
    // Simulate a process restart: empty in-memory map, same runs dir.
    // The first service releases its directory lock once background
    // tasks settle; poll briefly for handoff.
    let restarted = {
        let mut attempts = 0;
        loop {
            match crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None) {
                Ok(service) => {
                    break Arc::new(test_state(
                        service,
                        runs.clone(),
                        None,
                        tokio_util::sync::CancellationToken::new(),
                    ));
                }
                Err(error) if attempts < 100 => {
                    attempts += 1;
                    let _ = &error;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => panic!("restarted service should initialize: {error}"),
            }
        }
    };
    let retry = start_run(
        State(Arc::clone(&restarted)),
        headers.clone(),
        Json(request.clone()),
    )
    .await
    .expect("retry after restart must reuse the run");
    assert_eq!(
        retry.headers().get(header::LOCATION),
        Some(&first_location),
        "same key after restart must return the same run"
    );
    // E16: a replayed mutation response carries the same validator
    // as a conditional GET taken at the same instant (single
    // representation), so a client conditioning its next read on the
    // replay ETag observes no spurious change. Terminal snapshots are
    // byte-stable, so settle first, then replay once more and compare
    // that replay against a fresh GET.
    let run_id = first_location
        .to_str()
        .expect("location should be readable")
        .rsplit('/')
        .next()
        .expect("location should end with the run id")
        .to_string();
    for _ in 0..400 {
        if restarted
            .service
            .snapshot(run_id.clone())
            .await
            .expect("snapshot")
            .state
            .is_terminal()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let replay = start_run(State(Arc::clone(&restarted)), headers, Json(request))
        .await
        .expect("settled replay must converge");
    let get = crate::server::run_snapshot(
        State(Arc::clone(&restarted)),
        Path(run_id),
        HeaderMap::new(),
    )
    .await
    .expect("snapshot GET should succeed");
    assert_eq!(
        replay.headers().get(header::ETAG),
        get.headers().get(header::ETAG),
        "idempotent replay and GET must share one validator"
    );
    assert_eq!(
        restarted
            .service
            .list_run_items()
            .await
            .expect("runs should be listable")
            .len(),
        1,
        "retry must not create a duplicate run"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn concurrent_same_key_starts_converge_on_one_run() {
    // B02: two processes racing the same key must converge onto one
    // run: exactly one execution, identical locations, one run
    // directory. Separate memories force the durable protocol (no
    // in-memory short-circuit decides the race).
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("idempotency-race-{}", uuid::Uuid::now_v7()));
    let make_state = || {
        let service = service::LocalService::with_generator_roots_policy_and_store_mode(
            vec![workspace.join("fixtures/generators")],
            runs.clone(),
            None,
            policy::DEFAULT_MAX_ACTIVE_RUNS,
            policy::DEFAULT_MAX_TRACKED_RUNS,
            service::RunStoreMode::SharedFilesystem,
            service::ServiceDeploymentPolicy::default(),
        )
        .expect("shared service should initialize");
        Arc::new(test_state(
            service,
            runs.clone(),
            None,
            tokio_util::sync::CancellationToken::new(),
        ))
    };
    let before: std::collections::BTreeSet<String> =
        std::fs::read_dir(&runs).map_or(BTreeSet::new(), |entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        });
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("race-once"));
    let request = StartRun {
        generator_id: "hello-template".into(),
        inputs: BTreeMap::from([("name".into(), json!("test"))]),
        ..Default::default()
    };
    let state_a = make_state();
    let state_b = make_state();
    let headers_b = headers.clone();
    let request_b = request.clone();
    let (first, second) = tokio::join!(
        start_run(State(Arc::clone(&state_a)), headers, Json(request)),
        start_run(State(Arc::clone(&state_b)), headers_b, Json(request_b)),
    );
    let first = first.expect("first racer should respond");
    let second = second.expect("second racer should respond");
    assert_eq!(
        first.headers().get(header::LOCATION),
        second.headers().get(header::LOCATION),
        "concurrent same-key starts must return the same run"
    );
    let after: std::collections::BTreeSet<String> = std::fs::read_dir(&runs)
        .expect("runs dir should be readable")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        // The idempotency bookkeeping directory, the store lock, and
        // per-run admission locks are not runs.
        .filter(|name| {
            name != "idempotency" && name != ".service.lock" && !name.starts_with(".admission-")
        })
        .collect();
    let created: Vec<_> = after.difference(&before).collect();
    assert_eq!(
        created.len(),
        1,
        "concurrent same-key starts must create exactly one run, created: {created:?}"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn descending_history_pages_cover_newest_runs_without_gaps() {
    // B12: newest-first pages with a real advancing cursor. Three runs
    // across two pages must cover every id exactly once in
    // non-increasing time order.
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("history-desc-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    for _ in 0..3 {
        state
            .service
            .start_run(StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("test"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
    }
    let page = |cursor: Option<String>| {
        let state = Arc::clone(&state);
        async move {
            let response = list_runs(
                State(state),
                Query(api::RunListQuery {
                    limit: Some(2),
                    cursor,
                    order: Some(api::RunListOrder::Desc),
                    ..Default::default()
                }),
                HeaderMap::new(),
            )
            .await
            .expect("history page should list");
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .expect("list body should read");
            serde_json::from_slice::<api::RunListResponse>(&body).expect("list body should parse")
        }
    };
    let first = page(None).await;
    assert_eq!(first.items.len(), 2, "first page should hold two runs");
    let cursor = first
        .next_cursor
        .clone()
        .expect("first page should continue");
    let second = page(Some(cursor)).await;
    assert_eq!(
        second.items.len(),
        1,
        "second page should hold the last run"
    );
    assert!(second.next_cursor.is_none(), "history must end");
    let mut ids: Vec<_> = first
        .items
        .iter()
        .chain(second.items.iter())
        .map(|item| item.run_id.clone())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3, "pages must cover every run exactly once");
    let times: Vec<_> = first
        .items
        .iter()
        .chain(second.items.iter())
        .map(|item| item.started_at.clone())
        .collect();
    assert!(
        times.windows(2).all(|pair| pair[0] >= pair[1]),
        "descending pages must not go back in time"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn idempotent_fork_reuses_run_and_conflicts_on_digest() {
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("idempotency-fork-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let source_id = state
        .service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("source run should start");
    wait_for_terminal(&state.service, &source_id).await;
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("fork-once"));
    let body = ForkRun {
        at_seq: 1,
        state_patch: Default::default(),
        ..Default::default()
    };
    let first = fork_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Path(source_id.clone()),
        Json(body.clone()),
    )
    .await
    .expect("first fork should succeed");
    let second = fork_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Path(source_id.clone()),
        Json(body),
    )
    .await
    .expect("retried fork should reuse the run");
    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(
        first.headers().get(header::LOCATION),
        second.headers().get(header::LOCATION)
    );
    let divergent = ForkRun {
        at_seq: 2,
        state_patch: Default::default(),
        ..Default::default()
    };
    let conflict = fork_run(
        State(Arc::clone(&state)),
        headers,
        Path(source_id),
        Json(divergent),
    )
    .await
    .expect_err("reused key with different body must conflict");
    assert_eq!(conflict.problem.status, StatusCode::CONFLICT.as_u16());
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn idempotent_answer_replays_identical_payload() {
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("idempotency-answer-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let run_id = state
        .service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("question run should start");
    let question_id = wait_for_question(&state.service, &run_id).await;
    let mut headers = HeaderMap::new();
    headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("answer-once"));
    let payload = AnswerPayload {
        values: BTreeMap::from([("answer".into(), json!("brief"))]),
    };
    answer_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Path((run_id.clone(), question_id.clone())),
        Json(payload.clone()),
    )
    .await
    .expect("first answer should apply");
    answer_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Path((run_id.clone(), question_id.clone())),
        Json(payload),
    )
    .await
    .expect("retried answer should replay");
    let divergent = AnswerPayload {
        values: BTreeMap::from([("answer".into(), json!("detailed"))]),
    };
    let conflict = answer_run(
        State(Arc::clone(&state)),
        headers,
        Path((run_id, question_id)),
        Json(divergent),
    )
    .await
    .expect_err("reused key with different values must conflict");
    assert_eq!(conflict.problem.status, StatusCode::CONFLICT.as_u16());
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn cost_metrics_endpoint_reports_terminal_totals() {
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("cost-metrics-{}", uuid::Uuid::now_v7()));
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let run_id = state
        .service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal(&state.service, &run_id).await;
    let response = read_cost_metrics(
        State(Arc::clone(&state)),
        Path(run_id.clone()),
        HeaderMap::new(),
    )
    .await
    .expect("cost metrics should load");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("metrics body should read");
    let costs: api::RunCostMetrics =
        serde_json::from_slice(&body).expect("metrics body should parse");
    assert_eq!(costs.run_id, run_id);
    assert_eq!(costs.cost_usd, 0.0);
    assert!(costs.priced);
    assert_eq!(costs.metrics.llm_calls, 0);
    let _ = std::fs::remove_dir_all(&runs);
}

async fn wait_for_terminal(service: &LocalService, run_id: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let snapshot = service
                .snapshot(run_id.to_string())
                .await
                .expect("snapshot should load");
            if snapshot.state.is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("run should reach a terminal state");
}

async fn wait_for_question(service: &LocalService, run_id: &str) -> String {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let snapshot = service
                .snapshot(run_id.to_string())
                .await
                .expect("snapshot should load");
            if let Some(question) = snapshot.question {
                break question.id;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("run should ask a question")
}

#[tokio::test]
async fn ready_persist_failure_then_retry_reuses_the_original_run() {
    // E03: a Ready persist failure after execution must not orphan into a
    // second run: the retry with the same key and digest adopts the
    // original run instead of starting another execution. Full chain
    // through the real idempotency handler and real service (no mocks).
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root should exist")
        .to_path_buf();
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .expect("temporary directory path should be UTF-8")
        .join(format!("ready-fail-{}", uuid::Uuid::now_v7()));
    let _temp_guard = IdempotencyTempGuard(runs.clone());
    let state = Arc::new(test_state(
        crate::test_service(workspace.join("fixtures/generators"), runs.clone(), None)
            .expect("service should initialize"),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let request = StartRun {
        generator_id: "hello-template".into(),
        inputs: BTreeMap::from([("name".into(), json!("ready"))]),
        ..Default::default()
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        IDEMPOTENCY_HEADER,
        HeaderValue::from_static("ready-fail-run"),
    );
    let first = start_run(
        State(Arc::clone(&state)),
        headers.clone(),
        Json(request.clone()),
    )
    .await
    .expect("first request should start");
    let first_location = first
        .headers()
        .get(header::LOCATION)
        .expect("start should carry a location")
        .clone();
    {
        let idempotency_dir = runs.join("idempotency");
        if idempotency_dir.is_dir()
            && let Ok(entries) = std::fs::read_dir(&idempotency_dir)
        {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".json") && !name.ends_with(".pending.json") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        state.idempotency.lock().await.clear();
    }
    let retry = start_run(State(Arc::clone(&state)), headers, Json(request))
        .await
        .expect("retry after Ready loss must reuse the run");
    assert_eq!(
        retry.headers().get(header::LOCATION),
        Some(&first_location),
        "same key after Ready loss must return the same run"
    );
    assert_eq!(
        state
            .service
            .list_run_items()
            .await
            .expect("runs should be listable")
            .len(),
        1,
        "retry must not create a duplicate run"
    );
}
