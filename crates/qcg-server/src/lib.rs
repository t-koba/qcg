mod server;

pub use server::*;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Json;
    use axum::extract::{Path, Query, State};
    use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
    use camino::Utf8PathBuf;
    use qcg_api::{AnswerPayload, ForkRun, RunListQuery, StartRun};
    use qcg_service::{LocalQcgService, RunStoreMode};
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use qcg_policy::{IDEMPOTENCY_HEADER, IDEMPOTENCY_MAX_ENTRIES};
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
            .join(format!("qcg-router-routes-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
        });
        let config = ServerConfig {
            generators_dir: workspace.join("fixtures/generators"),
            providers_path: None,
            extra_generators_dirs: Vec::new(),
            runs_dir: runs.clone(),
            max_active_runs: qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            max_tracked_runs: qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            run_store_mode: RunStoreMode::Exclusive,
            cors_origins: Vec::new(),
            api_token: None,
            max_request_bytes: None,
            max_artifact_bytes: None,
            max_artifact_entries: None,
            max_asset_bytes: None,
        };
        let app = super::server::build_router(&state, &config).expect("router should build");

        // The OpenAPI table is the single canon: every entry must be well-formed
        // and unique so generated clients and docs cannot drift.
        let mut seen = BTreeSet::new();
        for route in qcg_api::API_ROUTES {
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
        for route in qcg_api::API_ROUTES {
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

    #[test]
    fn bearer_digest_comparison_rejects_any_difference() {
        let expected = sha256_bytes("correct-token");
        assert!(constant_time_digest_eq(
            &expected,
            &sha256_bytes("correct-token")
        ));
        assert!(!constant_time_digest_eq(
            &expected,
            &sha256_bytes("wrong-token")
        ));
    }

    #[tokio::test]
    async fn explicit_providers_path_is_authoritative() {
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-server-providers-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("listener should bind: {error}"),
        };
        let missing = root.join("providers.toml");
        let error = super::serve_with_listener(
            ServerConfig {
                generators_dir: root.join("generators"),
                providers_path: Some(missing.clone()),
                extra_generators_dirs: Vec::new(),
                runs_dir: root.join("runs"),
                max_active_runs: qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                max_tracked_runs: qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                run_store_mode: RunStoreMode::Exclusive,
                cors_origins: Vec::new(),
                api_token: None,
                max_request_bytes: None,
                max_artifact_bytes: None,
                max_artifact_entries: None,
                max_asset_bytes: None,
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
            .join(format!("qcg-idempotency-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
        });
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("same-run"));
        let request = StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
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
        let error = start_run(
            State(Arc::clone(&state)),
            headers,
            Json(StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
                ..Default::default()
            }),
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
            .join(format!("qcg-idempotency-restart-{}", uuid::Uuid::now_v7()));
        let make_state = || {
            Arc::new(AppState {
                service: LocalQcgService::new(
                    workspace.join("fixtures/generators"),
                    runs.clone(),
                    None,
                )
                .expect("service should initialize"),
                runs_dir: runs.clone(),
                oauth_origin: None,
                oauth_allowed_origins: BTreeSet::new(),
                oauth_callback_url: None,
                idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
                api_token_digest: None,
                artifact_limits: qcg_service::ArtifactZipLimits::default(),
                asset_limit: None,
                max_request_bytes: None,
            })
        };
        let state = make_state();
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("restart-run"));
        let request = StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
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
                match LocalQcgService::new(
                    workspace.join("fixtures/generators"),
                    runs.clone(),
                    None,
                ) {
                    Ok(service) => {
                        break Arc::new(AppState {
                            service,
                            runs_dir: runs.clone(),
                            oauth_origin: None,
                            oauth_allowed_origins: BTreeSet::new(),
                            oauth_callback_url: None,
                            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
                            api_token_digest: None,
                            artifact_limits: qcg_service::ArtifactZipLimits::default(),
                            asset_limit: None,
                            max_request_bytes: None,
                        });
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
        let retry = start_run(State(Arc::clone(&restarted)), headers, Json(request))
            .await
            .expect("retry after restart must reuse the run");
        assert_eq!(
            retry.headers().get(header::LOCATION),
            Some(&first_location),
            "same key after restart must return the same run"
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
    async fn idempotent_fork_reuses_run_and_conflicts_on_digest() {
        let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-idempotency-fork-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
        });
        let source_id = state
            .service
            .start_run(StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
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
            .join(format!("qcg-idempotency-answer-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
        });
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
            .join(format!("qcg-cost-metrics-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
        });
        let run_id = state
            .service
            .start_run(StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_terminal(&state.service, &run_id).await;
        let costs = read_cost_metrics(State(Arc::clone(&state)), Path(run_id.clone()))
            .await
            .expect("cost metrics should load");
        assert_eq!(costs.run_id, run_id);
        assert_eq!(costs.cost_usd, 0.0);
        assert!(costs.priced);
        assert_eq!(costs.metrics.llm_calls, 0);
        let _ = std::fs::remove_dir_all(&runs);
    }

    async fn wait_for_terminal(service: &LocalQcgService, run_id: &str) {
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

    async fn wait_for_question(service: &LocalQcgService, run_id: &str) -> String {
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
    async fn cancelled_idempotency_owner_releases_pending_entry() {
        let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .expect("workspace root should exist")
            .to_path_buf();
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-idempotency-cancel-{}", uuid::Uuid::now_v7()));
        let state = Arc::new(AppState {
            service: LocalQcgService::new(
                workspace.join("fixtures/generators"),
                runs.clone(),
                None,
            )
            .expect("service should initialize"),
            runs_dir: runs.clone(),
            oauth_origin: None,
            oauth_allowed_origins: BTreeSet::new(),
            oauth_callback_url: None,
            idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
            api_token_digest: None,
            artifact_limits: qcg_service::ArtifactZipLimits::default(),
            asset_limit: None,
            max_request_bytes: None,
        });
        let owner_id = uuid::Uuid::now_v7();
        let (completed, _) = tokio::sync::watch::channel(false);
        state.idempotency.lock().await.insert(
            "cancelled".into(),
            IdempotencyEntry::Pending {
                digest: "digest".into(),
                owner_id,
                created_at: Instant::now(),
                completed,
            },
        );
        drop(PendingIdempotencyGuard::new(
            Arc::clone(&state),
            "cancelled".into(),
            owner_id,
        ));
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if state.idempotency.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled pending entry should be released promptly");
    }

    #[test]
    fn asset_content_type_is_specific_when_known_and_binary_when_unknown() {
        assert_eq!(content_type_for_name("module.wasm"), "application/wasm");
        assert_eq!(content_type_for_name("font.woff2"), "font/woff2");
        assert_eq!(content_type_for_name("README"), "application/octet-stream");
        assert_eq!(
            content_type_for_name("data.weird"),
            "application/octet-stream"
        );
    }
}
