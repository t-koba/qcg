use super::support::*;
use crate::*;
use api::{AnswerPayload, ApiError, RunStatus, StartRun};
use camino::Utf8PathBuf;
use engine::JournalLimits;
use futures_util::StreamExt as _;
use serde_json::json;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn generator_detail_exposes_bounded_package_tree_identity() {
    let root = temp_run_dir("generator-detail-package-identity");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators.clone()], root.clone());
    let detail = service
        .describe("hello-template")
        .await
        .expect("generator detail should load");
    let expected = crate::package::package_identity_sha256(
        &generators.join("hello-template"),
        &crate::package::PackageLimits::default(),
    )
    .expect("fixture package identity should compute");
    assert_eq!(detail.package_sha256, expected);
    assert_eq!(detail.package_sha256.len(), 64);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn reviewed_package_mismatch_fails_before_run_side_effects() {
    let root = temp_run_dir("start-package-mismatch");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.clone());
    let detail = service.describe("hello-template").await.expect("detail");
    let wrong = if detail.package_sha256.starts_with('0') {
        format!("1{}", &detail.package_sha256[1..])
    } else {
        format!("0{}", &detail.package_sha256[1..])
    };
    let error = service
        .start_run_with_package_identity(
            StartRun {
                generator_id: "hello-template".into(),
                inputs: BTreeMap::from([("name".into(), json!("mismatch"))]),
                ..Default::default()
            },
            Some("reserved-test-run".into()),
            &wrong,
        )
        .await
        .expect_err("changed package identity must be refused");
    assert!(matches!(error, ApiError::Conflict { .. }));
    assert!(service.list_run_items().await.expect("runs").is_empty());
    assert!(!root.join("reserved-test-run").exists());
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn snapshot_and_cost_endpoint_expose_terminal_metrics() {
    let root = temp_run_dir("cost-metrics");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("metrics"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let terminal = wait_for_terminal_snapshot(&service, &id).await;
    let metrics = terminal
        .metrics
        .expect("terminal snapshot should carry metrics");
    assert_eq!(metrics.llm_calls, 0);
    assert_eq!(metrics.cost_microusd, 0);
    let costs = service
        .run_cost_metrics(id)
        .await
        .expect("cost metrics should load");
    assert_eq!(costs.cost_usd, 0.0);
    assert!(costs.priced);
    assert_eq!(costs.metrics.tokens_total, 0);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn concurrent_hitl_sessions_keep_answers_and_artifacts_isolated() {
    let root = temp_run_dir("concurrent-hitl");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        root.clone(),
        None,
        8,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let ids = futures_util::future::join_all((0..8).map(|_| {
        let service = service.clone();
        async move {
            service
                .start_run(StartRun {
                    generator_id: "ask-user".into(),
                    inputs: BTreeMap::new(),
                    ..Default::default()
                })
                .await
        }
    }))
    .await
    .into_iter()
    .map(|result| result.expect("HITL run should start"))
    .collect::<Vec<_>>();
    let waiting = futures_util::future::join_all(
        ids.iter()
            .map(|id| wait_for_snapshot(&service, id, RunStatus::Waiting)),
    )
    .await;
    let expected = waiting
        .iter()
        .enumerate()
        .map(|(index, snapshot)| {
            (
                snapshot.run_id.clone(),
                snapshot
                    .question
                    .as_ref()
                    .expect("run should expose its own question")
                    .id
                    .clone(),
                if index % 2 == 0 { "brief" } else { "detailed" },
            )
        })
        .collect::<Vec<_>>();
    let answers =
        futures_util::future::join_all(expected.iter().map(|(run_id, question_id, answer)| {
            let service = service.clone();
            let run_id = run_id.clone();
            let question_id = question_id.clone();
            let answer = (*answer).to_string();
            async move {
                service
                    .answer(
                        run_id,
                        question_id,
                        AnswerPayload {
                            values: BTreeMap::from([("answer".into(), json!(answer))]),
                        },
                    )
                    .await
            }
        }))
        .await;
    assert!(answers.into_iter().all(|result| result.is_ok()));
    for (run_id, _, answer) in expected {
        let snapshot = wait_for_terminal_snapshot(&service, &run_id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let artifact = std::fs::read_to_string(
            run_workspace_dir(
                &service
                    .run_dir_for(&run_id)
                    .await
                    .expect("run directory should exist"),
            )
            .join("answer.txt"),
        )
        .expect("HITL artifact should be readable");
        assert_eq!(artifact, format!("mode={answer}"));
    }
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn shared_journal_poll_rejects_event_body_above_limit_with_newline() {
    let run_dir = temp_run_dir("shared-journal-event-limit");
    let _ = std::fs::remove_dir_all(&run_dir);
    let meta_dir = run_meta_dir(&run_dir);
    std::fs::create_dir_all(&meta_dir).expect("run metadata should be created");
    // Explicit max only: the poller enforces the given journal event bound.
    let limits = JournalLimits {
        max_event_bytes: Some(16),
        max_total_bytes: None,
        max_event_count: None,
        max_state_bytes: None,
        scan_window_bytes: None,
    };
    let mut oversized = vec![b' '; 17];
    oversized.push(b'\n');
    std::fs::write(meta_dir.join("journal.jsonl"), oversized)
        .expect("oversized journal event should be written");

    let mut events = poll_journal_events_with_limits(
        run_dir.clone(),
        "oversized-run".into(),
        0,
        limits,
        policy::JOURNAL_POLL_INTERVAL_MILLIS,
        CancellationToken::new(),
    );
    // E05: a poll failure ends with a `stream_error` marker first, so
    // consumers distinguish failure-close from terminal-close, then the
    // stream closes.
    let failure = tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
        .await
        .expect("poller should emit a failure marker after rejecting the event")
        .expect("failure marker should be delivered");
    assert_eq!(
        failure.kind, "stream_error",
        "a poll failure must end with a failure marker, not a terminal event"
    );
    assert!(
        !api::is_terminal_event_kind(failure.kind.as_str()),
        "the failure marker must never fake a normal terminal event"
    );
    let end = tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
        .await
        .expect("poller should close after the failure marker");
    assert!(end.is_none());
    std::fs::remove_dir_all(run_dir).expect("temporary run should be removed");
}

#[tokio::test]
async fn oversized_observation_stream_is_refused_before_delivery() {
    // P0-05: the merged journal view must never allocate the observation
    // stream without a bound. An audit file above the run's effective
    // limit is refused with TooLarge at open, exactly like the durable
    // stream, instead of being read whole and merged.
    let runs = temp_run_dir("audit-stream-bound");
    let _ = std::fs::remove_dir_all(&runs);
    let _temp_guard = TempGuard(runs.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let id = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        RunStatus::Succeeded
    );
    let run_dir = service
        .run_dir_for(&id)
        .await
        .expect("run directory should resolve");
    let opened = service
        .open_journal_stream(id.clone())
        .await
        .expect("a run within its observation bound must open");
    assert_eq!(
        opened.audit_limit,
        Some(policy::DEFAULT_MAX_AUDIT_TOTAL_BYTES),
        "an unset `[audit].max_bytes` must resolve to the hard default, never unbounded"
    );
    assert!(
        opened.audit_len <= opened.audit_limit.unwrap_or(0) as u64,
        "the fixture observation stream must start inside its bound"
    );

    // One byte over the effective limit must be refused, not truncated.
    let audit_path = run_meta_dir(&run_dir).join("audit.jsonl");
    let oversized = vec![b'x'; policy::DEFAULT_MAX_AUDIT_TOTAL_BYTES + 1];
    std::fs::write(&audit_path, &oversized).expect("oversized observation stream should write");
    match service.open_journal_stream(id.clone()).await {
        Err(ApiError::TooLarge {
            limit_bytes,
            actual_bytes,
        }) => {
            assert_eq!(limit_bytes, policy::DEFAULT_MAX_AUDIT_TOTAL_BYTES);
            assert_eq!(actual_bytes, oversized.len());
        }
        other => panic!("an oversized observation stream must be refused, got {other:?}"),
    }
}

#[tokio::test]
async fn snapshot_exposes_generator_id_without_parsing_run_id() {
    // C03: snapshots carry the generator id explicitly so UUID hyphens
    // never leak into parsed ids.
    let runs = temp_run_dir("snapshot-generator-id");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let id = start_ask_user_and_answer(&service, "brief").await;
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.generator_id.as_str(), "ask-user");
    assert!(
        !snapshot.generator_id.contains('-') || snapshot.generator_id == "ask-user",
        "generator id must not contain UUID fragments"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn cost_metrics_treat_unmetered_search_as_unpriced() {
    let root = temp_run_dir("search-priced");
    let _ = std::fs::remove_dir_all(&root);
    let service = test_service(vec![], root.clone());
    let contract_dir = root.join("contract");
    std::fs::create_dir_all(&contract_dir).unwrap();
    std::fs::write(
        contract_dir.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "search-fixture"
name = "Search Fixture"
version = "0.1.0"

[llm]
max_tokens = 1024
[llm.model]
provider = "provider"
model = "model"
input_cost_per_million_usd = 2.0
output_cost_per_million_usd = 10.0

[[flow]]
id = "emit"
type = "write"
params = { content = "x", output_file = "x.txt" }
"#,
    )
    .unwrap();
    let generator_path = contract_dir.as_str();
    let meta = root.join("search-call").join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), "search-call", false, None)
            .unwrap();
    journal
        .event(
            "run_started",
            json!({"generator": "search-fixture@0.1.0",
                "generator_path": generator_path,
                "contract_sha256": "test", "inputs": {},
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock"}),
        )
        .unwrap();
    journal
        .event(
            "llm_call",
            json!({"node": "generate", "provider": "provider", "model": "model",
                "max_tokens": 16,
                "tokens": {"input": 100, "output": 20, "cached_input": 0},
                "cost_microusd": 42,
                "web_search": {"calls": null, "cost_microusd": null}}),
        )
        .unwrap();
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    let costs = service
        .run_cost_metrics("search-call".into())
        .await
        .expect("cost metrics should load");
    assert!(!costs.priced, "unmetered hosted search must report risk");

    // Liability-only shape: no billed call at all, only a failed route whose
    // hosted search was not metered. `pricing_covered` returns true early on an
    // empty call list, so the marker in `run_cost_metrics` alone must keep the
    // run unpriced.
    let meta = root.join("search-route-failed").join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal = engine::JournalWriter::create(
        &meta.join("journal.jsonl"),
        "search-route-failed",
        false,
        None,
    )
    .unwrap();
    journal
        .event(
            "run_started",
            json!({"generator": "search-fixture@0.1.0",
                "generator_path": generator_path,
                "contract_sha256": "test", "inputs": {},
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock"}),
        )
        .unwrap();
    // A search liability makes the engine write this otherwise observational
    // event durably. The writer keeps that choice internal, so append the
    // durable record here; the next writer event resynchronizes from it.
    let mut line = serde_json::to_vec(&json!({
        "t": "llm_route_failed",
        "seq": 2,
        "ts": "2026-11-02T00:00:00Z",
        "run_id": "search-route-failed",
        "trace_id": api::trace_id_for_run("search-route-failed"),
        "span_id": api::span_id_for_seq(2),
        "parent_span_id": api::span_id_for_scope("search-route-failed", "step:generate"),
        "node": "generate", "provider": "provider", "model": "model",
        "attempt": 1, "kind": {"http_status": 503},
        "web_search": {"calls": null, "cost_microusd": null}
    }))
    .unwrap();
    line.push(b'\n');
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(meta.join("journal.jsonl"))
        .unwrap();
    std::io::Write::write_all(&mut file, &line).unwrap();
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    let view = service
        .read_view(root.join("search-route-failed"))
        .await
        .expect("the liability-only run should read");
    assert!(
        view.state.budget.unresolved_search_cost,
        "the route-failure event must mark unresolved hosted-search cost"
    );
    let costs = service
        .run_cost_metrics("search-route-failed".into())
        .await
        .expect("cost metrics should load");
    assert!(
        !costs.priced,
        "a liability-only run with unmetered search must stay unpriced"
    );
    let _ = std::fs::remove_dir_all(&root);
}
