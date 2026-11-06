use super::*;
use crate::server::artifact_response::ArtifactResponseBudget;
use axum::response::Response;

const CAP: u64 = 1024 * 1024;

async fn open_state(generators: &Utf8PathBuf, runs: &Utf8PathBuf) -> Arc<AppState> {
    // Dropped service workers may briefly retain the exclusive store lock.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match crate::test_service(generators.clone(), runs.clone(), None) {
                Ok(service) => {
                    let mut state = test_state(
                        service,
                        runs.clone(),
                        None,
                        tokio_util::sync::CancellationToken::new(),
                    );
                    state.artifact_response_budget =
                        ArtifactResponseBudget::new(Some(CAP)).unwrap();
                    break Arc::new(state);
                }
                Err(error) => {
                    assert!(error.to_string().contains("lock"), "{error}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await
    .expect("service store lock should be released")
}

async fn setup() -> (IdempotencyTempGuard, Utf8PathBuf, Arc<AppState>, String) {
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("post-snapshot-budget-{}", uuid::Uuid::now_v7()));
    let guard = IdempotencyTempGuard(runs.clone());
    let state = open_state(&generators, &runs).await;
    let source = state
        .service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_terminal(&state.service, &source).await;
    (guard, generators, state, source)
}

// Exercise the production handlers, including digest lookup, durable commit,
// snapshot fetching and HTTP body ownership, rather than a response helper.
async fn post(
    state: &Arc<AppState>,
    source: &str,
    fork: bool,
    key: Option<&str>,
) -> Result<Response, (u16, Option<HeaderValue>)> {
    let mut headers = HeaderMap::new();
    if let Some(key) = key {
        headers.insert(IDEMPOTENCY_HEADER, key.parse().unwrap());
    }
    let result = if fork {
        fork_run(
            State(state.clone()),
            headers,
            Path(source.into()),
            Json(ForkRun {
                at_seq: 1,
                ..Default::default()
            }),
        )
        .await
    } else {
        add_package_identity_header(&mut headers, &state.service, "hello-template").await;
        start_run(
            State(state.clone()),
            headers,
            Json(StartRun {
                generator_id: "hello-template".into(),
                ..Default::default()
            }),
        )
        .await
    };
    result.map_err(|error| (error.problem.status, error.location))
}

fn run_id(location: &HeaderValue) -> String {
    location
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .into()
}

async fn charged_bytes(state: &AppState, response: Response) -> axum::body::Bytes {
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let etag = response.headers()[header::ETAG].clone();
    let bytes = axum::body::to_bytes(response.into_body(), CAP as usize)
        .await
        .unwrap();
    assert_eq!(etag, body_etag(&bytes));
    let rest = state
        .artifact_response_budget
        .reserve(CAP - bytes.len() as u64)
        .unwrap();
    assert_eq!(
        state
            .artifact_response_budget
            .reserve(1)
            .unwrap_err()
            .problem
            .status,
        503
    );
    drop(rest);
    bytes
}

#[tokio::test]
async fn initial_post_snapshots_charge_and_keep_ready_after_exhaustion() {
    let (_guard, _, state, source) = setup().await;
    for fork in [false, true] {
        // Unkeyed creation must take the same charged path.
        let response = post(&state, &source, fork, None).await.unwrap();
        let id = run_id(&response.headers()[header::LOCATION]);
        let bytes = charged_bytes(&state, response).await;
        drop(bytes);
        drop(state.artifact_response_budget.reserve(CAP).unwrap());
        wait_for_terminal(&state.service, &id).await;

        let key = if fork {
            "initial-fork"
        } else {
            "initial-start"
        };
        let count = state.service.list_run_items().await.unwrap().len();
        let full = state.artifact_response_budget.reserve(CAP).unwrap();
        let (status, location) = post(&state, &source, fork, Some(key)).await.unwrap_err();
        assert_eq!(status, 503);
        let location = location.expect("committed run location survives response failure");
        let id = run_id(&location);
        wait_for_terminal(&state.service, &id).await;
        assert_eq!(
            state.service.list_run_items().await.unwrap().len(),
            count + 1
        );
        state.idempotency.lock().await.clear(); // Retry must find durable Ready.
        assert_eq!(
            post(&state, &source, fork, Some(key)).await.unwrap_err().0,
            503
        );
        drop(full);
        let response = post(&state, &source, fork, Some(key)).await.unwrap();
        assert_eq!(response.headers()[header::LOCATION], location);
        let bytes = charged_bytes(&state, response).await;
        assert_eq!(
            &bytes[..],
            state
                .service
                .snapshot_json(id, &state.artifact_limits)
                .await
                .unwrap()
        );
        drop(bytes);
        drop(state.artifact_response_budget.reserve(CAP).unwrap());
        assert_eq!(
            state.service.list_run_items().await.unwrap().len(),
            count + 1
        );
    }
}

#[tokio::test]
async fn ready_post_snapshots_share_budget_across_routes_and_restart() {
    let (guard, generators, mut state, source) = setup().await;
    let mut locations = Vec::new();
    for fork in [false, true] {
        let response = post(
            &state,
            &source,
            fork,
            Some(if fork { "fork" } else { "start" }),
        )
        .await
        .unwrap();
        let location = response.headers()[header::LOCATION].clone();
        let id = run_id(&location);
        // A successful initial keyed response is charged too.
        drop(charged_bytes(&state, response).await);
        wait_for_terminal(&state.service, &id).await;
        locations.push(location);
    }
    let mut expected = Vec::new();
    for location in &locations {
        expected.push(
            state
                .service
                .snapshot_json(run_id(location), &state.artifact_limits)
                .await
                .unwrap(),
        );
    }
    let artifact_size = std::fs::metadata(guard.0.join(&source).join("workspace/README.md"))
        .unwrap()
        .len();
    let cap = expected.iter().map(|bytes| bytes.len() as u64).sum::<u64>() + artifact_size;
    for mode in ["memory", "durable", "restart"] {
        match mode {
            "durable" => state.idempotency.lock().await.clear(),
            "restart" => {
                drop(state);
                state = open_state(&generators, &guard.0).await;
                assert!(state.idempotency.lock().await.is_empty());
            }
            _ => {}
        }
        Arc::get_mut(&mut state).unwrap().artifact_response_budget =
            ArtifactResponseBudget::new(Some(cap)).unwrap();
        let (start, fork, artifact) = tokio::join!(
            post(&state, &source, false, Some("start")),
            post(&state, &source, true, Some("fork")),
            read_artifact(
                State(state.clone()),
                Path((source.clone(), "README.md".into())),
                HeaderMap::new()
            ),
        );
        let start = start.unwrap();
        let fork = fork.unwrap();
        let artifact = artifact.unwrap();
        for (response, location, bytes) in [
            (&start, &locations[0], &expected[0]),
            (&fork, &locations[1], &expected[1]),
        ] {
            assert_eq!(response.status(), StatusCode::CREATED);
            assert_eq!(&response.headers()[header::LOCATION], location);
            assert_eq!(response.headers()[header::ETAG], body_etag(bytes));
        }
        assert_eq!(
            state
                .artifact_response_budget
                .reserve(1)
                .unwrap_err()
                .problem
                .status,
            503
        );
        assert_eq!(
            post(&state, &source, false, Some("start"))
                .await
                .unwrap_err()
                .0,
            503
        );
        assert_eq!(
            post(&state, &source, true, Some("fork"))
                .await
                .unwrap_err()
                .0,
            503
        );
        assert_eq!(
            run_artifacts(State(state.clone()), Path(source.clone()), HeaderMap::new())
                .await
                .unwrap_err()
                .problem
                .status,
            503
        );
        drop(artifact);
        drop(
            state
                .artifact_response_budget
                .reserve(artifact_size)
                .unwrap(),
        );
        // Dropping an unpolled POST body admits a GET snapshot of identical size.
        drop(start);
        let get = run_snapshot(
            State(state.clone()),
            Path(run_id(&locations[0])),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        drop(get);
        let bytes = axum::body::to_bytes(fork.into_body(), CAP as usize)
            .await
            .unwrap();
        assert_eq!(&bytes[..], expected[1]);
        let rest = state
            .artifact_response_budget
            .reserve(cap - bytes.len() as u64)
            .unwrap();
        assert_eq!(
            state
                .artifact_response_budget
                .reserve(1)
                .unwrap_err()
                .problem
                .status,
            503
        );
        drop(rest);
        drop(bytes);
        drop(state.artifact_response_budget.reserve(cap).unwrap());

        // Both aggregate and per-response rejection release all capacity and
        // leave Ready intact. Exact retry succeeds after restoring capacity.
        for fork in [false, true] {
            let key = if fork { "fork" } else { "start" };
            let size = expected[usize::from(fork)].len() as u64;
            Arc::get_mut(&mut state).unwrap().artifact_response_budget =
                ArtifactResponseBudget::new(Some(size - 1)).unwrap();
            assert_eq!(
                post(&state, &source, fork, Some(key)).await.unwrap_err().0,
                413
            );
            drop(state.artifact_response_budget.reserve(size - 1).unwrap());
            Arc::get_mut(&mut state).unwrap().artifact_response_budget =
                ArtifactResponseBudget::new(Some(cap)).unwrap();
            Arc::get_mut(&mut state).unwrap().artifact_limits.max_bytes = Some(size - 1);
            assert_eq!(
                post(&state, &source, fork, Some(key)).await.unwrap_err().0,
                413
            );
            Arc::get_mut(&mut state).unwrap().artifact_limits.max_bytes = None;
            drop(state.artifact_response_budget.reserve(cap).unwrap());
            drop(post(&state, &source, fork, Some(key)).await.unwrap());
            drop(state.artifact_response_budget.reserve(cap).unwrap());
        }
        assert_eq!(state.service.list_run_items().await.unwrap().len(), 3);
    }
}

#[test]
fn snapshot_builder_error_releases_reservation() {
    let budget = ArtifactResponseBudget::new(Some(4)).unwrap();
    let result = json_response_with_etag(
        StatusCode::CREATED,
        b"null".to_vec(),
        Some(budget.reserve(4).unwrap()),
        Some("invalid\nlocation".into()),
    );
    assert_eq!(result.unwrap_err().problem.status, 500);
    drop(budget.reserve(4).unwrap());
}
