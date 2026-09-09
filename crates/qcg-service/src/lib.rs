pub mod package;
pub use package::{ArtifactZipLimits, PackageLimits};

mod artifacts;
mod catalog;
mod lifecycle;
mod queue;
mod run_dirs;
mod runs_api;
mod summaries;
mod types;

pub use artifacts::*;
pub use run_dirs::*;
pub use summaries::*;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use camino::{Utf8Path, Utf8PathBuf};
    #[cfg(unix)]
    use fs2::FileExt as _;
    use futures_util::StreamExt as _;
    use qcg_api::ConfirmDecision;
    use qcg_api::ConfirmationDecision;
    use qcg_api::{
        AnswerPayload, ApiError, ForkRun, ForkStatePatch, RunSnapshot, RunStatus, StartRun,
    };
    use qcg_engine::JournalLimits;
    use qcg_policy::DEFAULT_MAX_TRACKED_RUNS;
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, BTreeSet};
    #[cfg(unix)]
    use std::fs::OpenOptions;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    fn temp_run_dir(name: &str) -> Utf8PathBuf {
        let dir =
            std::env::temp_dir().join(format!("qcg-service-test-{name}-{}", uuid::Uuid::now_v7()));
        Utf8PathBuf::from_path_buf(dir).expect("temporary directory path must be UTF-8")
    }

    async fn read_journal_string(service: &LocalQcgService, id: String) -> String {
        use tokio::io::AsyncReadExt as _;
        let mut opened = service
            .open_journal_stream(id)
            .await
            .expect("journal should open");
        let mut bytes = Vec::new();
        opened
            .file
            .read_to_end(&mut bytes)
            .await
            .expect("journal should read");
        String::from_utf8(bytes).expect("journal should be UTF-8")
    }

    #[tokio::test]
    async fn queued_snapshots_expose_admission_order() {
        let root = temp_run_dir("queue-visibility");
        let _ = std::fs::remove_dir_all(&root);
        write_generator_package(&root.join("generators"), "queue-gen");
        let service = LocalQcgService::new(root.join("generators"), root.join("runs"), None)
            .expect("service should initialize");
        let contract = service
            .load_generator("queue-gen")
            .expect("fixture generator should load");
        // Insert out of order to prove positions follow admission time.
        let first_at = chrono::Utc::now();
        let second_at = first_at + chrono::Duration::seconds(5);
        for (run_id, at, state) in [
            ("q-second", Some(second_at), RunStatus::Queued),
            ("q-first", Some(first_at), RunStatus::Queued),
            ("q-running", None, RunStatus::Running),
        ] {
            let run_dir = root.join("runs").join(run_id);
            prepare_api_run_directory(&run_dir).expect("run directory should prepare");
            let (events, _) = broadcast::channel(512);
            let record = RunRecord {
                contract: contract.clone(),
                contract_sha256: contract.sha256.clone(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                priority: 0,
                parent_run_id: None,
                preempted: false,
                state,
                run_dir: run_dir.clone(),
                artifacts: None,
                question: None,
                confirm: None,
                events,
                cancellation: CancellationToken::new(),
                task: Arc::new(Mutex::new(None)),
                queued_at: at,
                owner_id: String::new(),
                ephemeral: false,
            };
            write_run_event(
                &record,
                "run_queued",
                json!({
                    "run_id": run_id,
                    "generator": "queue-gen@0.1.0",
                    "generator_path": contract.root,
                    "contract_sha256": contract.sha256,
                    "inputs": {},
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                }),
            )
            .expect("queued event should append");
            service
                .inner
                .runs
                .write()
                .await
                .insert(run_id.into(), record);
        }
        let first = service
            .snapshot("q-first".into())
            .await
            .expect("first snapshot should load");
        assert_eq!(first.state, RunStatus::Queued);
        assert_eq!(first.queue_position, Some(1));
        // Display follows the durable journal instant, not the memory value,
        // so every process reports identical admission order.
        let first_journal_at =
            crate::summaries::read_last_queued_at(&root.join("runs").join("q-first"))
                .expect("journal should carry admission time")
                .to_rfc3339();
        assert_eq!(first.queued_at, Some(first_journal_at));
        let second = service
            .snapshot("q-second".into())
            .await
            .expect("second snapshot should load");
        assert_eq!(second.queue_position, Some(2));
        let second_journal_at =
            crate::summaries::read_last_queued_at(&root.join("runs").join("q-second"))
                .expect("journal should carry admission time")
                .to_rfc3339();
        assert_eq!(second.queued_at, Some(second_journal_at));
        let running = service
            .snapshot("q-running".into())
            .await
            .expect("running snapshot should load");
        assert_eq!(running.queue_position, None);
        assert_eq!(running.queued_at, None);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn ids_reject_platform_specific_paths_and_traversal() {
        for id in [
            "",
            "/",
            "/etc/passwd",
            "\\etc\\passwd",
            "\\\\server\\share",
            "C:/Windows/System32",
            "C:\\Windows\\System32",
            "C:Windows/System32",
            "../generator",
            "nested/../generator",
            "nested\\..\\generator",
            ".",
            "./generator",
            "nested//generator",
        ] {
            assert!(
                !is_safe_id(id),
                "{id:?} must not be accepted as an identifier"
            );
        }
        for id in ["generator", "generator.v1", "nested/generator"] {
            assert!(is_safe_id(id), "{id:?} should be accepted as an identifier");
        }
    }

    fn write_generator_package(root: &Utf8Path, id: &str) {
        let dir = root.join(id);
        std::fs::create_dir_all(&dir).expect("package directory should be created");
        std::fs::write(
            dir.join("qcg.toml"),
            format!(
                r#"
[generator]
id = "{id}"
name = "{id}"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[permissions]
"#
            ),
        )
        .expect("manifest should be written");
    }

    #[test]
    fn explicit_providers_path_is_loaded_without_fallback() {
        let root = temp_run_dir("explicit-providers");
        let _ = std::fs::remove_dir_all(&root);
        let providers_path = root.join("providers.toml");
        let provider_id = format!("explicit-{}", std::process::id());
        std::fs::create_dir_all(&root).expect("provider directory should be created");
        std::fs::write(
            &providers_path,
            format!(
                r#"
[[provider]]
id = "{provider_id}"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#
            ),
        )
        .expect("providers registry should be written");

        let service = LocalQcgService::new(
            root.join("generators"),
            root.join("runs"),
            Some(providers_path.clone()),
        )
        .expect("service should load the explicit providers registry");
        assert!(
            service
                .inner
                .llm_runtime
                .provider
                .capabilities_for(&provider_id)
                .is_some(),
            "the explicitly selected provider should be registered"
        );

        let missing = root.join("missing.toml");
        let error = LocalQcgService::new(
            root.join("generators"),
            root.join("other-runs"),
            Some(missing.clone()),
        )
        .expect_err("an explicit missing path must not fall back to another registry");
        assert!(error.to_string().contains(missing.as_str()));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn generator_roots_merge_with_first_root_winning() {
        let primary = temp_run_dir("roots-primary");
        let secondary = temp_run_dir("roots-secondary");
        let runs = temp_run_dir("roots-runs");
        let _ = std::fs::remove_dir_all(&primary);
        let _ = std::fs::remove_dir_all(&secondary);
        let _ = std::fs::remove_dir_all(&runs);
        write_generator_package(&primary, "shared");
        write_generator_package(&primary, "only-primary");
        // The secondary copy of `shared` must never shadow the primary one.
        write_generator_package(&secondary, "shared");
        write_generator_package(&secondary, "bundled-demo");

        let service = LocalQcgService::with_generator_roots(
            vec![primary.clone(), secondary.clone()],
            runs.clone(),
            None,
        )
        .expect("service should initialize");

        let mut listed: Vec<String> = service
            .list_generators()
            .await
            .expect("generators should be listed")
            .into_iter()
            .map(|generator| generator.id)
            .collect();
        listed.sort();
        assert_eq!(listed, vec!["bundled-demo", "only-primary", "shared"]);

        let shared = service
            .load_generator("shared")
            .expect("primary should win");
        assert!(
            shared.root.starts_with(&primary),
            "duplicate id must resolve from the first root, got {}",
            shared.root
        );

        let fallback = service
            .load_generator("bundled-demo")
            .expect("secondary root should resolve");
        assert_eq!(fallback.manifest.generator.id, "bundled-demo");
        assert!(service.load_generator("absent").is_err());

        for dir in [primary, secondary, runs] {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn artifact_zip_rejects_sources_above_limit() {
        let run_dir = temp_run_dir("zip-limit");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("large.txt"), "abcdef")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "large.txt",
                    "sha256": "unused",
                    "bytes": 6,
                    "label": "Large",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limits(
            &run_dir,
            &mut bytes,
            &ArtifactZipLimits {
                max_bytes: Some(5),
                max_entries: None,
            },
        )
        .expect_err("zip generation should reject oversized artifacts");
        assert!(error.to_string().contains("exceed limit"));
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn artifact_zip_rejects_compressed_output_above_limit() {
        let run_dir = temp_run_dir("zip-output-limit");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("a.txt"), "a")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "a.txt",
                    "sha256": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
                    "bytes": 1,
                    "label": "A",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limits(
            &run_dir,
            &mut bytes,
            &ArtifactZipLimits {
                max_bytes: Some(32),
                max_entries: None,
            },
        )
        .expect_err("zip generation should reject oversized compressed output");
        assert!(
            error.to_string().contains("artifact zip output exceeds"),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn artifact_zip_rejects_manifest_byte_mismatch_before_writing() {
        let run_dir = temp_run_dir("zip-bytes-mismatch");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("result.txt"), "ok")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "result.txt",
                    "sha256": "2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df",
                    "bytes": 3,
                    "label": "Result",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limits(
            &run_dir,
            &mut bytes,
            &ArtifactZipLimits {
                max_bytes: Some(128),
                max_entries: None,
            },
        )
        .expect_err("zip generation should reject a byte-count mismatch");
        assert!(error.to_string().contains("bytes mismatch"));
        assert!(
            bytes.get_ref().is_empty(),
            "zip output must not start before validation"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn artifact_zip_rejects_manifest_sha256_mismatch_before_writing() {
        let run_dir = temp_run_dir("zip-sha256-mismatch");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("result.txt"), "ok")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "result.txt",
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "bytes": 2,
                    "label": "Result",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limits(
            &run_dir,
            &mut bytes,
            &ArtifactZipLimits {
                max_bytes: Some(128),
                max_entries: None,
            },
        )
        .expect_err("zip generation should reject a sha256 mismatch");
        assert!(error.to_string().contains("sha256 mismatch"));
        assert!(
            bytes.get_ref().is_empty(),
            "zip output must not start before validation"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn concurrent_start_runs_have_unique_ids_and_independent_outputs() {
        let root = temp_run_dir("concurrent-start");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "concurrent"
name = "Concurrent"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "marker"
required = true
type = "string"

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "{{ inputs.marker }}"

"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_and_max_active_runs(
            vec![root.clone()],
            runs.clone(),
            None,
            10,
        )
        .expect("service should initialize");
        let results = futures_util::future::join_all((0..10).map(|index| {
            let service = service.clone();
            let marker = format!("marker-{index}");
            async move {
                let id = service
                    .start_run(StartRun {
                        generator_id: "generator".into(),
                        inputs: BTreeMap::from([(String::from("marker"), json!(marker.clone()))]),
                        ..Default::default()
                    })
                    .await?;
                Ok::<_, ApiError>((id, marker))
            }
        }))
        .await;
        let runs_with_markers: Vec<(String, String)> = results
            .into_iter()
            .map(|result| result.expect("concurrent run should start"))
            .collect();
        let ids: Vec<String> = runs_with_markers.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(ids.len(), BTreeSet::<String>::from_iter(ids.clone()).len());
        let snapshots = futures_util::future::join_all(
            runs_with_markers
                .iter()
                .map(|(id, _)| wait_for_terminal_snapshot(&service, id)),
        )
        .await;
        for ((id, marker), snapshot) in runs_with_markers.iter().zip(snapshots) {
            assert_eq!(snapshot.run_id, *id);
            assert_eq!(snapshot.state, RunStatus::Succeeded);
            let run_dir = service
                .run_dir_for(id)
                .await
                .expect("run directory should exist");
            assert_eq!(run_dir, runs.join(id));
            assert_eq!(
                std::fs::read_to_string(run_workspace_dir(&run_dir).join("result.txt"))
                    .expect("run artifact should be readable"),
                *marker
            );

            let events = read_journal_events(&run_dir).expect("journal should be valid JSONL");
            assert!(!events.is_empty(), "journal should contain run events");
            let seqs: Vec<u64> = events
                .iter()
                .map(|event| {
                    event
                        .get("seq")
                        .and_then(Value::as_u64)
                        .expect("journal event should have a sequence")
                })
                .collect();
            assert_eq!(
                seqs,
                (1..=events.len() as u64).collect::<Vec<_>>(),
                "run journal sequence should be contiguous for {id}"
            );
            assert_eq!(
                snapshot.seq,
                *seqs.last().expect("journal should have a last seq")
            );
            let started = events
                .iter()
                .find(|event| event.get("t").and_then(Value::as_str) == Some("run_started"))
                .expect("journal should start the run");
            assert_eq!(
                started.get("run_id").and_then(Value::as_str),
                Some(id.as_str())
            );
            assert_eq!(
                started
                    .get("inputs")
                    .and_then(|inputs| inputs.get("marker"))
                    .and_then(Value::as_str),
                Some(marker.as_str())
            );
            let terminal_events: Vec<&Value> = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.get("t").and_then(Value::as_str),
                        Some("run_finished")
                            | Some("run_error")
                            | Some("run_canceled")
                            | Some("run_interrupted")
                    )
                })
                .collect();
            assert_eq!(
                terminal_events.len(),
                1,
                "run should have one terminal event"
            );
            assert_eq!(
                terminal_events[0].get("t").and_then(Value::as_str),
                Some("run_finished")
            );
            assert!(
                read_run_events(&run_dir)
                    .expect("typed journal events should parse")
                    .iter()
                    .all(|event| event.run_id == *id),
                "all events should remain scoped to {id}"
            );
        }

        let listed = service
            .list_run_items()
            .await
            .expect("runs should be listable");
        assert_eq!(
            listed
                .iter()
                .filter(|snapshot| ids.contains(&snapshot.run_id))
                .count(),
            10
        );
    }

    #[tokio::test]
    async fn snapshot_and_cost_endpoint_expose_terminal_metrics() {
        let root = temp_run_dir("cost-metrics");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, root.clone(), None)
            .expect("service should initialize");
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
        let service = LocalQcgService::with_generator_roots_and_max_active_runs(
            vec![generators],
            root.clone(),
            None,
            8,
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

    #[cfg(unix)]
    #[tokio::test]
    async fn run_limits_bound_active_and_tracked_runs() {
        let root = temp_run_dir("max-active-runs");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "max-active"
name = "Max Active"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "active run limit test", isolation = "trusted_host" }]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone()],
            runs,
            None,
            1,
            2,
            RunStoreMode::Exclusive,
        )
        .expect("service should initialize");
        let fork_source_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("fork source should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &fork_source_id)
                .await
                .state,
            RunStatus::Succeeded
        );
        let fork_source_dir = service
            .run_dir_for(&fork_source_id)
            .await
            .expect("fork source directory should exist");
        let fork_seq = read_journal_events(&fork_source_dir)
            .expect("fork source journal should be readable")
            .into_iter()
            .find(|event| event.get("t").and_then(Value::as_str) == Some("step_finished"))
            .and_then(|event| event.get("seq").and_then(Value::as_u64))
            .expect("fork source should contain a stable step checkpoint");
        let first_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("first run should start");
        let second_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("second run should enter the durable queue");
        let capacity_error = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect_err("tracked run capacity should reject another queued run");
        assert!(
            capacity_error
                .to_string()
                .contains("run capacity is exhausted")
        );
        let fork_capacity_error = service
            .fork_run(
                &fork_source_id,
                ForkRun {
                    at_seq: fork_seq,
                    state_patch: ForkStatePatch::default(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("fork must share the tracked run capacity");
        assert!(
            fork_capacity_error
                .to_string()
                .contains("run capacity is exhausted")
        );
        assert_eq!(
            service
                .snapshot(second_id.clone())
                .await
                .expect("queued run should be visible")
                .state,
            RunStatus::Queued
        );
        assert_eq!(
            wait_for_terminal_snapshot(&service, &first_id).await.state,
            RunStatus::Succeeded
        );
        assert_eq!(
            wait_for_terminal_snapshot(&service, &second_id).await.state,
            RunStatus::Succeeded
        );
        let replacement_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("terminal runs should be evicted when capacity is reused");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &replacement_id)
                .await
                .state,
            RunStatus::Succeeded
        );
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
        };
        let mut oversized = vec![b' '; 17];
        oversized.push(b'\n');
        std::fs::write(meta_dir.join("journal.jsonl"), oversized)
            .expect("oversized journal event should be written");

        let mut events =
            poll_journal_events_with_limits(run_dir.clone(), "oversized-run".into(), 0, limits);
        let next = tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
            .await
            .expect("poller should terminate after rejecting the event");
        assert!(next.is_none());
        std::fs::remove_dir_all(run_dir).expect("temporary run should be removed");
    }

    #[test]
    fn runs_directory_is_exclusive_between_services() {
        let root = temp_run_dir("runs-directory-lock");
        let _ = std::fs::remove_dir_all(&root);
        let generators = root.join("generators");
        let runs = root.join("runs");
        let first = LocalQcgService::new(generators.clone(), runs.clone(), None)
            .expect("first service should acquire the runs directory");
        let error = LocalQcgService::new(generators.clone(), runs.clone(), None)
            .expect_err("a second service must not share the runs directory");
        assert!(
            error.to_string().contains("already owned"),
            "lock failure should explain ownership conflict, got {error}"
        );
        drop(first);
        let second = LocalQcgService::new(generators, runs, None)
            .expect("the runs directory should be reusable after the first service drops");
        drop(second);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_filesystem_store_allows_multiple_services_and_serializes_each_run() {
        let root = temp_run_dir("shared-runs-directory");
        let _ = std::fs::remove_dir_all(&root);
        let generators = root.join("generators");
        let runs = root.join("runs");
        let first = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("first shared service should initialize");
        let second = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![generators],
            runs,
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("second shared service should initialize");

        let run_dir = root.join("shared-run");
        let first_lease = try_lock_run_execution(&run_dir)
            .expect("first lease attempt should succeed")
            .expect("first process should own the run");
        assert!(
            try_lock_run_execution(&run_dir)
                .expect("second lease attempt should be valid")
                .is_none(),
            "a run must have only one execution owner"
        );
        drop(first_lease);
        assert!(
            try_lock_run_execution(&run_dir)
                .expect("released lease should be reusable")
                .is_some()
        );
        drop(first);
        drop(second);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_runs_reject_the_same_active_output_directory() {
        let root = temp_run_dir("direct-output-lock");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let output = root.join("output");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "direct-output-lock"
name = "Direct Output Lock"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "direct output lock test", isolation = "trusted_host" }]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root.clone(), root.join("runs"), None)
            .expect("service should initialize");
        let direct_run = || DirectRun {
            generator_path: generator.clone(),
            inputs: BTreeMap::new(),
            output_dir: output.clone(),
            json_events: false,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            llm_seed_override: None,
        };
        let first = tokio::spawn({
            let service = service.clone();
            let run = direct_run();
            async move { service.run_generator_path(run).await }
        });
        let lock_path = direct_run_meta_dir(&output).join(".run.lock");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(probe) = OpenOptions::new().read(true).write(true).open(&lock_path) {
                    match probe.try_lock_exclusive() {
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(error) => panic!("output lock probe failed: {error}"),
                        Ok(()) => {}
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first direct run should acquire its output lock");
        let error = service
            .run_generator_path(direct_run())
            .await
            .expect_err("a second direct run must not share an active output directory");
        assert!(error.to_string().contains("already active"));
        first
            .await
            .expect("first direct run task should not panic")
            .expect("first direct run should succeed");
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_stops_a_running_service_command_before_following_nodes() {
        let root = temp_run_dir("service-cancel");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "cancelable"
name = "Cancelable"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "cancellation test", isolation = "trusted_host" }]

[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]

[[flow]]
id = "must_not_run"
type = "write"
needs = ["sleep"]
artifact = { label = "Unexpected", required = false }
[flow.params]
output_file = "must-not-exist.txt"
content = "unexpected"

"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("cancelable run should start");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        service
            .cancel(id.clone())
            .await
            .expect("cancel should succeed");
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should exist");
        let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .expect("journal should be complete when cancel returns");
        assert!(journal.contains("\"t\":\"run_canceled\""));
        assert_eq!(
            service
                .snapshot(id)
                .await
                .expect("canceled snapshot should be available")
                .state,
            RunStatus::Canceled
        );
        assert!(
            !run_workspace_dir(&run_dir)
                .join("must-not-exist.txt")
                .exists()
        );
        assert!(
            !crate::run_dirs::has_pending_cancel_control(&run_dir),
            "settled cancellation must consume its mailbox control files"
        );
    }

    #[tokio::test]
    async fn completion_racing_with_cancel_commits_exactly_one_terminal_state() {
        let root = temp_run_dir("completion-cancel-race");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "completion-cancel-race"
name = "Completion Cancel Race"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = false }
[flow.params]
output_file = "result.txt"
content = "complete"

"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root.clone(), runs, None).expect("service should initialize");

        for _ in 0..32 {
            let id = service
                .start_run(StartRun {
                    generator_id: "generator".into(),
                    inputs: BTreeMap::new(),
                    ..Default::default()
                })
                .await
                .expect("run should start");
            tokio::task::yield_now().await;
            service
                .cancel(id.clone())
                .await
                .expect("cancel should settle the run");
            let snapshot = service
                .snapshot(id.clone())
                .await
                .expect("settled snapshot should be available");
            assert!(matches!(
                snapshot.state,
                RunStatus::Succeeded | RunStatus::Canceled
            ));
            let run_dir = service
                .run_dir_for(&id)
                .await
                .expect("run directory should exist");
            let events = read_journal_events(&run_dir).expect("journal should be valid JSONL");
            let terminal_count = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.get("t").and_then(Value::as_str),
                        Some("run_finished")
                            | Some("run_error")
                            | Some("run_canceled")
                            | Some("run_interrupted")
                    )
                })
                .count();
            assert_eq!(terminal_count, 1);
        }
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resume_after_confirmation_replays_prior_steps_and_runs_side_effect_once() {
        let service = LocalQcgService::new(
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators"),
            temp_run_dir("confirmation-resume"),
            None,
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "side-effect-confirm".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
        let confirm = snapshot.confirm.expect("run should request confirmation");
        service
            .confirm(
                id.clone(),
                confirm.id.clone(),
                ConfirmDecision {
                    decision: ConfirmationDecision::Approve,
                },
            )
            .await
            .expect("confirmation should resume run");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let journal = read_journal_string(&service, id).await;
        let approved_effects = journal
            .lines()
            .filter(|line| {
                line.contains("\"t\":\"side_effect\"")
                    && line.contains("\"node\":\"effect\"")
                    && line.contains("\"decision\":\"approved_by_user\"")
            })
            .count();
        assert_eq!(
            approved_effects, 1,
            "confirmed side effect must execute once"
        );
        assert!(!confirm.id.is_empty());
    }

    #[tokio::test]
    async fn foreach_items_accepts_expressions() {
        let root = temp_run_dir("foreach-expression");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator dir should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[blocks.site]]
id = "write_site"
type = "write"

[blocks.site.params]
content = "site={{ item }}"
output_file = "sites/{{ item }}.txt"

[[flow]]
id = "emit_sites"
type = "foreach"

[flow.params]
items = "reverse(inputs.sites)"
max_iterations = 10
subflow = "site"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "sites"
item_type = "string"
required = true
type = "list"

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("sites".into(), json!(["a", "b"]))]),
                ..Default::default()
            })
            .await
            .expect("expression foreach should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let workspace = run_workspace_dir(&runs.join(&id));
        assert_eq!(
            std::fs::read_to_string(workspace.join("sites/a.txt")).expect("site a should render"),
            "site=a"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("sites/b.txt")).expect("site b should render"),
            "site=b"
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn priority_preempts_and_resumes_in_order() {
        let root = temp_run_dir("priority-preemption");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator dir should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "nap"
type = "command"

[flow.params]
command = ["sleep", "60"]

[permissions]
fs_write = ["workspace"]
side_effects = "allowed"

[[permissions.commands]]
bin = "sleep"
args = ["60"]
purpose = "priority scheduling test"
isolation = "trusted_host"

[runtime]
command_timeout_seconds = 300
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
        )
        .expect("service should initialize");
        let start_prioritized = async |service: &LocalQcgService, priority: i32| {
            service
                .start_run(StartRun {
                    generator_id: "generator".into(),
                    inputs: BTreeMap::new(),
                    priority: Some(priority),
                    ..Default::default()
                })
                .await
                .expect("prioritized run should start")
        };
        let running = start_prioritized(&service, 0).await;
        assert_eq!(
            wait_for_snapshot(&service, &running, RunStatus::Running)
                .await
                .state,
            RunStatus::Running
        );
        let queued_equal = start_prioritized(&service, 0).await;
        assert_eq!(
            wait_for_snapshot(&service, &queued_equal, RunStatus::Queued)
                .await
                .state,
            RunStatus::Queued
        );
        // A higher-priority arrival preempts the running run instead of
        // waiting behind it.
        let urgent = start_prioritized(&service, 5).await;
        assert_eq!(
            wait_for_snapshot(&service, &urgent, RunStatus::Running)
                .await
                .state,
            RunStatus::Running
        );
        assert_eq!(
            wait_for_snapshot(&service, &running, RunStatus::Queued)
                .await
                .state,
            RunStatus::Queued
        );
        let snapshot = service
            .snapshot(queued_equal.clone())
            .await
            .expect("queued snapshot should load");
        assert_eq!(snapshot.queue_position, Some(1));
        let snapshot = service
            .snapshot(running.clone())
            .await
            .expect("preempted snapshot should load");
        assert_eq!(snapshot.queue_position, Some(2));
        // The preempted writer must have exited before the requeue append:
        // seqs stay unique and monotonic across the handoff.
        let journal = read_journal_string(&service, running.clone()).await;
        let seqs: Vec<u64> = journal
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .expect("journal line should be JSON")
                    .get("seq")
                    .and_then(Value::as_u64)
                    .expect("event should carry seq")
            })
            .collect();
        assert!(!seqs.is_empty());
        let mut deduped = seqs.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            seqs.len(),
            "preempted journal seqs must be unique across the writer handoff"
        );
        assert!(
            seqs.windows(2).all(|pair| pair[0] < pair[1]),
            "preempted journal seqs must stay monotonic"
        );
        // Finishing the urgent run resumes equals in FIFO order.
        service
            .cancel(urgent.clone())
            .await
            .expect("urgent run should cancel");
        assert_eq!(
            wait_for_snapshot(&service, &queued_equal, RunStatus::Running)
                .await
                .state,
            RunStatus::Running
        );
        service
            .cancel(queued_equal.clone())
            .await
            .expect("queued run should cancel");
        assert_eq!(
            wait_for_snapshot(&service, &running, RunStatus::Running)
                .await
                .state,
            RunStatus::Running
        );
        service
            .cancel(running.clone())
            .await
            .expect("preempted run should cancel");
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn fork_snapshot_carries_parent_link() {
        let runs = temp_run_dir("fork-parent");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let source = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("source run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &source).await.state,
            RunStatus::Succeeded
        );
        let checkpoint =
            read_journal_events(&service.run_dir_for(&source).await.expect("source dir"))
                .expect("source journal")
                .into_iter()
                .filter_map(|event| {
                    (event.get("t").and_then(Value::as_str) == Some("step_finished"))
                        .then(|| event.get("seq").and_then(Value::as_u64))
                        .flatten()
                })
                .max()
                .expect("source should have a finished step");
        let fork = service
            .fork_run(
                &source,
                ForkRun {
                    at_seq: checkpoint,
                    ..Default::default()
                },
            )
            .await
            .expect("fork should start");
        let snapshot = wait_for_terminal_snapshot(&service, &fork).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        assert_eq!(snapshot.parent_run_id.as_deref(), Some(source.as_str()));
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn partial_fork_is_not_mistaken_for_completed_admission() {
        // A03: a crash between checkpoint copy and the fork's own
        // `run_queued` leaves a journal whose (rewritten) source
        // `run_queued` predates `run_forked`. Adoption must wipe it for a
        // deterministic redo instead of resuming the partial fork.
        let runs = temp_run_dir("fork-partial-adopt");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let source = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("source run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &source).await.state,
            RunStatus::Succeeded
        );
        let source_dir = service
            .run_dir_for(&source)
            .await
            .expect("source dir should resolve");
        let checkpoint = read_journal_events(&source_dir)
            .expect("source journal")
            .into_iter()
            .filter_map(|event| {
                (event.get("t").and_then(Value::as_str) == Some("step_finished"))
                    .then(|| event.get("seq").and_then(Value::as_u64))
                    .flatten()
            })
            .max()
            .expect("source should have a finished step");
        let fork_id = format!("ask-user-fork-{}", uuid::Uuid::now_v7());
        let fork_dir = runs.join(&fork_id);
        // Simulate the crash window: checkpoint copy done, own `run_queued`
        // never appended.
        crate::run_dirs::prepare_checkpoint_fork(
            &source_dir,
            &source,
            &fork_dir,
            &fork_id,
            checkpoint,
            &ForkStatePatch::default(),
        )
        .expect("checkpoint copy should succeed");
        assert!(
            !crate::run_dirs::try_adopt_run_dir(&fork_dir, &fork_id)
                .expect("adoption check should succeed"),
            "partial fork must not adopt"
        );
        assert!(
            !fork_dir.exists(),
            "partial fork must be wiped for a deterministic redo"
        );
        // The same reserved id now converges through a fresh fork.
        let fork = service
            .fork_run_with_id(
                &source,
                ForkRun {
                    at_seq: checkpoint,
                    ..Default::default()
                },
                Some(fork_id.clone()),
            )
            .await
            .expect("fork redo should start");
        assert_eq!(fork, fork_id);
        let snapshot = wait_for_terminal_snapshot(&service, &fork).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        assert_eq!(snapshot.parent_run_id.as_deref(), Some(source.as_str()));
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn await_node_joins_sibling_runs() {
        let root = temp_run_dir("await-join");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone(), generators],
            runs.clone(),
            None,
            8,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
        )
        .expect("service should initialize");
        let child = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("child run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &child).await.state,
            RunStatus::Succeeded
        );
        let waiter_root = root.join("waiter");
        std::fs::create_dir_all(&waiter_root).expect("waiter dir should be created");
        std::fs::write(
            waiter_root.join("qcg.toml"),
            format!(
                r#"
[generator]
id = "waiter"
name = "Waiter"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "join_children"
type = "await"

[flow.params]
runs = ["{child}"]
timeout_secs = 30

[[flow]]
id = "done"
type = "write"
needs = ["join_children"]
artifact = {{ label = "Done", required = true }}

[flow.params]
content = "joined"
output_file = "done.txt"

[permissions]
fs_write = ["workspace"]
"#,
            ),
        )
        .expect("waiter manifest should be written");
        let waiter = service
            .start_run(StartRun {
                generator_id: "waiter".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("waiter run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &waiter).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let unknown_root = root.join("waiter-unknown");
        std::fs::create_dir_all(&unknown_root).expect("waiter dir should be created");
        std::fs::write(
            unknown_root.join("qcg.toml"),
            r#"
[generator]
id = "waiter-unknown"
name = "Waiter Unknown"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "join_missing"
type = "await"

[flow.params]
runs = ["does-not-exist"]

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("waiter manifest should be written");
        let unknown = service
            .start_run(StartRun {
                generator_id: "waiter-unknown".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("waiter run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &unknown).await.state,
            RunStatus::Failed
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn delete_run_removes_terminal_runs_and_rejects_active_ones() {
        let runs = temp_run_dir("delete-run");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let finished = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &finished).await.state,
            RunStatus::Succeeded
        );
        let waiting = service
            .start_run(StartRun {
                generator_id: "side-effect-confirm".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("confirming run should start");
        assert_eq!(
            wait_for_snapshot(&service, &waiting, RunStatus::Confirming)
                .await
                .state,
            RunStatus::Confirming
        );
        let conflict = service
            .delete_run(&waiting)
            .await
            .expect_err("active runs must not be deletable");
        assert!(conflict.to_string().contains("still active"));
        service
            .delete_run(&finished)
            .await
            .expect("terminal runs should be deletable");
        assert!(!runs.join(&finished).exists());
        assert!(runs.join(&waiting).exists());
        let missing = service
            .delete_run(&finished)
            .await
            .expect_err("repeated deletes must report not-found");
        assert!(missing.to_string().contains("was not found"));
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn run_bundle_contains_snapshot_journal_and_artifacts() {
        let runs = temp_run_dir("run-bundle");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &id).await.state,
            RunStatus::Succeeded
        );
        let parts = service
            .run_bundle_parts(&id)
            .await
            .expect("bundle parts should assemble");
        let mut bundle = Vec::new();
        write_run_bundle_stream_with_limits(
            &parts.snapshot,
            &parts.inputs,
            &parts.journal,
            parts.outputs.as_ref(),
            &parts.verified,
            &mut bundle,
            &ArtifactZipLimits::default(),
        )
        .expect("bundle should write");
        let archive = zip::ZipArchive::new(std::io::Cursor::new(bundle))
            .expect("bundle should be a valid zip");
        let mut names = archive.file_names().map(str::to_string).collect::<Vec<_>>();
        names.sort();
        assert!(names.contains(&"snapshot.json".to_string()), "{names:?}");
        assert!(names.contains(&"inputs.json".to_string()), "{names:?}");
        assert!(names.contains(&"journal.jsonl".to_string()), "{names:?}");
        assert!(names.contains(&"outputs.json".to_string()), "{names:?}");
        assert!(
            names.contains(&"artifacts/answer.txt".to_string()),
            "{names:?}"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn preprovisioned_answers_complete_run_without_interaction() {
        let runs = temp_run_dir("preprovisioned-answers");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("pre-provisioned run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        assert!(snapshot.question.is_none());
        let workspace = run_workspace_dir(&runs.join(&id));
        assert!(
            std::fs::read_to_string(workspace.join("answer.txt"))
                .expect("answer artifact should exist")
                .contains("mode=brief")
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn digest_bound_confirmation_authorizes_the_exact_operation() {
        let runs = temp_run_dir("preprovisioned-confirmations");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "side-effect-confirm".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        // Legacy node-wide `effect:command` approvals no longer exist: only
        // the digest-bound confirmation journaled for this exact operation
        // authorizes it.
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
        let confirm = snapshot.confirm.expect("run should request confirmation");
        assert!(
            confirm.id.starts_with("effect:command:"),
            "confirmation must bind the operation digest"
        );
        service
            .confirm(
                id.clone(),
                confirm.id.clone(),
                ConfirmDecision {
                    decision: ConfirmationDecision::Approve,
                },
            )
            .await
            .expect("confirmation should resume run");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        assert!(snapshot.confirm.is_none());
        let journal = read_journal_string(&service, id).await;
        assert!(
            journal
                .lines()
                .any(|line| line.contains("\"t\":\"side_effect\"")
                    && line.contains("\"node\":\"effect\"")
                    && line.contains("\"decision\":\"approved_by_user\"")
                    && !line.contains("approved_by_bulk")),
            "approved side effect must execute"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn foreach_questions_and_confirmations_are_scoped_per_iteration() {
        let root = temp_run_dir("foreach-interactions");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "foreach-interactions"
name = "Foreach Interactions"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "items"
type = "list"
item_type = "string"
required = true
min_items = 1

[[flow]]
id = "each"
type = "foreach"

[flow.params]
items = "inputs.items"
subflow = "item"
max_iterations = 10
parallel = 1

[[flow]]
id = "done"
type = "write"
artifact = { label = "Done", required = true }

[flow.params]
output_file = "done.txt"
content = "done"

[[blocks.item]]
id = "ask"
type = "ask_user"

[blocks.item.params]
content = "Approve {{ item }}"
options = ["yes"]

[[blocks.item]]
id = "effect"
type = "command"

[blocks.item.params]
command = ["echo", "{{ item }}"]

[permissions]
fs_write = ["workspace"]
side_effects = "confirm"

[[permissions.commands]]
bin = "echo"
args = ["*"]
purpose = "record each confirmed iteration"
isolation = "trusted_host"
"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root.clone(), runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("items".into(), json!(["alpha", "beta"]))]),
                ..Default::default()
            })
            .await
            .expect("foreach run should start");

        for (index, item) in ["alpha", "beta"].into_iter().enumerate() {
            let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
            let question = snapshot.question.expect("iteration should ask a question");
            assert_eq!(question.id, format!("each[{index}]/ask"));
            service
                .answer(
                    id.clone(),
                    question.id,
                    AnswerPayload {
                        values: BTreeMap::from([("answer".into(), json!("yes"))]),
                    },
                )
                .await
                .expect("iteration answer should resume the run");

            let snapshot = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
            let confirm = snapshot
                .confirm
                .expect("iteration should request side-effect confirmation");
            assert!(
                confirm
                    .id
                    .starts_with(&format!("each[{index}]/effect:command:")),
                "confirm id must bind the operation digest, got `{}`",
                confirm.id
            );
            assert_eq!(confirm.target, format!("echo {item}"));
            assert!(
                !confirm.operation_digest.is_empty(),
                "confirm must carry the operation digest"
            );
            service
                .confirm(
                    id.clone(),
                    confirm.id,
                    ConfirmDecision {
                        decision: ConfirmationDecision::Approve,
                    },
                )
                .await
                .expect("iteration confirmation should resume the run");
        }

        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let journal = read_journal_string(&service, id).await;
        for index in 0..2 {
            let node = format!("\"node\":\"each[{index}]/effect\"");
            assert_eq!(
                journal
                    .lines()
                    .filter(|line| {
                        line.contains("\"t\":\"side_effect\"")
                            && line.contains(&node)
                            && line.contains("\"decision\":\"approved_by_user\"")
                    })
                    .count(),
                1,
                "each iteration side effect must be approved exactly once"
            );
        }
    }

    #[tokio::test]
    async fn nested_foreach_restores_parent_item_and_addresses_every_iteration() {
        let root = temp_run_dir("nested-foreach");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "nested-foreach"
name = "Nested Foreach"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "outer"
type = "list"
item_type = "string"
required = true
min_items = 1

[[inputs.stages.fields]]
id = "inner"
type = "list"
item_type = "string"
required = true
min_items = 1

[[flow]]
id = "outer"
type = "foreach"

[flow.params]
items = "inputs.outer"
subflow = "outer_body"
max_iterations = 4
parallel = 1

[[blocks.outer_body]]
id = "inner"
type = "foreach"

[blocks.outer_body.params]
items = "inputs.inner"
subflow = "inner_body"
max_iterations = 4
parallel = 1

[[blocks.outer_body]]
id = "write_outer"
type = "write"

[blocks.outer_body.params]
output_file = "outer-{{ item }}.txt"
content = "{{ item }}"

[[blocks.inner_body]]
id = "write_inner"
type = "write"

[blocks.inner_body.params]
output_file = "inner-{{ item }}.txt"
content = "{{ item }}"
"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root, runs.clone(), None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([
                    ("outer".into(), json!(["a", "b"])),
                    ("inner".into(), json!(["x", "y"])),
                ]),
                ..Default::default()
            })
            .await
            .expect("nested foreach run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Succeeded).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let workspace = run_workspace_dir(&runs.join(&id));
        assert_eq!(
            std::fs::read_to_string(workspace.join("outer-a.txt")).unwrap(),
            "a"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("outer-b.txt")).unwrap(),
            "b"
        );
        let journal = std::fs::read_to_string(run_meta_dir(&runs.join(&id)).join("journal.jsonl"))
            .expect("journal should be readable");
        assert!(journal.contains("outer[0]/inner[0]/write_inner"));
        assert!(journal.contains("outer[1]/inner[1]/write_inner"));
    }

    #[tokio::test]
    async fn waiting_run_survives_gc_and_rehydrates_after_restart() {
        let runs = temp_run_dir("waiting-rehydrate");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("interactive run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot
            .question
            .expect("run should have a pending question");
        assert!(
            gc_run_directories(&runs, 0, true)
                .expect("GC should inspect runs")
                .is_empty(),
            "GC must not delete a waiting run"
        );
        assert!(runs.join(&id).is_dir());
        drop(service);

        let restored = LocalQcgService::new(generators, runs, None)
            .expect("service should rehydrate waiting run");
        let snapshot = restored
            .snapshot(id.clone())
            .await
            .expect("rehydrated snapshot should exist");
        assert_eq!(snapshot.state, RunStatus::Waiting);
        assert_eq!(
            snapshot.question.as_ref().map(|item| &item.id),
            Some(&question.id)
        );
        restored
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect("rehydrated run should accept its answer");
        let snapshot = wait_for_terminal_snapshot(&restored, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_racing_with_an_answer_has_one_canceled_terminal_state() {
        let root = temp_run_dir("cancel-answer-race");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "cancel-answer-race"
name = "Cancel Answer Race"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 30"]

[permissions]
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "hold resumed run", isolation = "trusted_host" }]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot
            .question
            .expect("run should be waiting for an answer");
        let (cancel_result, answer_result) = tokio::join!(
            service.cancel(id.clone()),
            service.answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
        );
        cancel_result.expect("cancel should succeed");
        if let Err(error) = answer_result {
            assert!(error.to_string().contains("not waiting for user input"));
        }
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Canceled).await;
        assert_eq!(snapshot.state, RunStatus::Canceled);
        let journal = read_journal_string(&service, id).await;
        assert_eq!(journal.matches("\"t\":\"run_canceled\"").count(), 1);
        assert!(!journal.contains("\"status\":\"success\",\"t\":\"run_finished\""));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_workspace_blocks_resume_without_repeating_side_effects() {
        let root = temp_run_dir("missing-workspace-resume");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "missing-workspace-resume"
name = "Missing Workspace Resume"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "write_before"
type = "write"

[flow.params]
output_file = "before.txt"
content = "before"

[[flow]]
id = "effect"
type = "command"

[flow.params]
command = ["echo", "effect"]

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "echo", args = ["effect"], purpose = "resume safety proof", isolation = "trusted_host" }]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot
            .question
            .expect("run should wait after the side effect");
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should exist");
        std::fs::remove_dir_all(run_workspace_dir(&run_dir))
            .expect("workspace should be removed for the recovery test");
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("answer should trigger a guarded resume");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Failed);
        let journal = read_journal_string(&service, id).await;
        assert_eq!(
            journal
                .lines()
                .filter(|line| {
                    line.contains("\"t\":\"side_effect\"")
                        && line.contains("\"node\":\"effect\"")
                        && line.contains("\"decision\":\"allowed\"")
                })
                .count(),
            1,
            "resume failure must not repeat a completed side effect"
        );
        assert!(journal.contains("output `before.txt` is unavailable"));
    }

    #[tokio::test]
    async fn checkpoint_fork_restores_files_and_resumes_with_an_explicit_state_patch() {
        let root = temp_run_dir("checkpoint-fork-generators");
        let runs = temp_run_dir("checkpoint-fork-runs");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&runs);
        let generator = root.join("generator");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "generator"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[resources.docs]
type = "dir"
path = "docs"
llm_visible = true

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "marker"
required = true
type = "string"

[[flow]]
id = "before"
type = "write"
artifact = { label = "Before", required = true }
[flow.params]
output_file = "before.txt"
content = "{{ inputs.marker }}"

[[flow]]
id = "after"
type = "write"
needs = ["before"]
artifact = { label = "After", required = true }
[flow.params]
output_file = "after.txt"
content = "{{ inputs.marker }}"
"#,
        )
        .expect("generator manifest should be written");
        std::fs::create_dir_all(generator.join("docs/nested"))
            .expect("directory resource should be created");
        std::fs::write(generator.join("docs/guide.md"), "guide")
            .expect("directory resource file should be written");
        std::fs::write(generator.join("docs/nested/reference.md"), "reference")
            .expect("nested directory resource file should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let source_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("marker".into(), json!("source"))]),
                ..Default::default()
            })
            .await
            .expect("source run should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &source_id).await.state,
            RunStatus::Succeeded
        );
        let source_dir = service
            .run_dir_for(&source_id)
            .await
            .expect("source run directory");
        let checkpoint_seq = read_journal_events(&source_dir)
            .expect("source journal")
            .into_iter()
            .find(|event| {
                event.get("t").and_then(Value::as_str) == Some("step_finished")
                    && event.get("node").and_then(Value::as_str) == Some("before")
            })
            .and_then(|event| event.get("seq").and_then(Value::as_u64))
            .expect("before checkpoint sequence");
        let fork_id = service
            .fork_run(
                &source_id,
                ForkRun {
                    at_seq: checkpoint_seq,
                    state_patch: ForkStatePatch {
                        inputs: BTreeMap::from([("marker".into(), json!("fork"))]),
                        ..ForkStatePatch::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("checkpoint fork should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &fork_id).await.state,
            RunStatus::Succeeded
        );
        let fork_dir = service
            .run_dir_for(&fork_id)
            .await
            .expect("fork run directory");
        assert_eq!(
            std::fs::read_to_string(run_workspace_dir(&fork_dir).join("before.txt"))
                .expect("restored checkpoint file"),
            "source"
        );
        assert_eq!(
            std::fs::read_to_string(run_workspace_dir(&fork_dir).join("after.txt"))
                .expect("resumed output file"),
            "fork"
        );
        let journal = read_journal_events(&fork_dir).expect("fork journal");
        assert!(journal.iter().any(|event| {
            event.get("t").and_then(Value::as_str) == Some("resource")
                && event.get("name").and_then(Value::as_str) == Some("docs")
                && event
                    .get("files")
                    .and_then(Value::as_array)
                    .is_some_and(|files| files.len() == 2)
        }));
        assert!(journal.iter().any(|event| {
            event.get("t").and_then(Value::as_str) == Some("run_forked")
                && event.get("source_run_id").and_then(Value::as_str) == Some(source_id.as_str())
                && event.get("source_seq").and_then(Value::as_u64) == Some(checkpoint_seq)
        }));
        assert!(run_meta_dir(&source_dir).join("checkpoint-blobs").is_dir());
    }

    async fn wait_for_snapshot(
        service: &LocalQcgService,
        id: &str,
        state: RunStatus,
    ) -> RunSnapshot {
        for _ in 0..200 {
            let snapshot = service
                .snapshot(id.to_string())
                .await
                .expect("run snapshot should exist");
            if snapshot.state == state {
                return snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("run did not reach state `{state}`");
    }

    async fn wait_for_terminal_snapshot(service: &LocalQcgService, id: &str) -> RunSnapshot {
        for _ in 0..200 {
            let snapshot = service
                .snapshot(id.to_string())
                .await
                .expect("run snapshot should exist");
            if snapshot.state.is_terminal() {
                return snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("run did not reach a terminal state");
    }

    #[test]
    fn artifact_zip_contains_manifest_artifacts() {
        let run_dir = temp_run_dir("zip-file");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::create_dir_all(run_workspace_dir(&run_dir).join("reports"))
            .expect("artifact directory should be written");
        std::fs::write(run_workspace_dir(&run_dir).join("reports/result.txt"), "ok")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "reports/result.txt",
                    "sha256": "2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df",
                    "bytes": 2,
                    "label": "Result",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let bytes = read_artifacts_zip(&run_dir).expect("zip should be written");
        assert!(!run_dir.join("artifacts.zip.tmp").exists());
        assert!(!run_dir.join("artifacts.zip").exists());
        let mut archive =
            zip::ZipArchive::new(Cursor::new(bytes)).expect("zip archive should parse");
        assert!(
            archive
                .by_name("reports/")
                .expect("directory entry")
                .is_dir()
        );
        let mut entry = archive
            .by_name("reports/result.txt")
            .expect("artifact should be present");
        assert!(
            entry.last_modified().expect("artifact timestamp").year() > 1980,
            "artifact timestamp must come from source metadata"
        );
        let mut text = String::new();
        std::io::Read::read_to_string(&mut entry, &mut text).expect("artifact should read as text");
        assert_eq!(text, "ok");
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn unknown_admission_time_sorts_last() {
        let root = temp_run_dir("queue-unknown-last");
        let _ = std::fs::remove_dir_all(&root);
        write_generator_package(&root.join("generators"), "queue-gen");
        let service = LocalQcgService::new(root.join("generators"), root.join("runs"), None)
            .expect("service should initialize");
        let contract = service
            .load_generator("queue-gen")
            .expect("fixture generator should load");
        let first_at = chrono::Utc::now();
        for (run_id, at) in [("q-first", Some(first_at)), ("q-unknown", None)] {
            let run_dir = root.join("runs").join(run_id);
            prepare_api_run_directory(&run_dir).expect("run directory should prepare");
            let (events, _) = broadcast::channel(512);
            let record = RunRecord {
                contract: contract.clone(),
                contract_sha256: contract.sha256.clone(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                priority: 0,
                parent_run_id: None,
                preempted: false,
                state: RunStatus::Queued,
                run_dir: run_dir.clone(),
                artifacts: None,
                question: None,
                confirm: None,
                events,
                cancellation: CancellationToken::new(),
                task: Arc::new(Mutex::new(None)),
                queued_at: at,
                owner_id: String::new(),
                ephemeral: false,
            };
            write_run_event(
                &record,
                "run_queued",
                json!({
                    "run_id": run_id,
                    "generator": "queue-gen@0.1.0",
                    "generator_path": contract.root,
                    "contract_sha256": contract.sha256,
                    "inputs": {},
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                }),
            )
            .expect("queued event should append");
            service
                .inner
                .runs
                .write()
                .await
                .insert(run_id.into(), record);
        }
        let first = service
            .snapshot("q-first".into())
            .await
            .expect("first snapshot should load");
        assert_eq!(first.queue_position, Some(1));
        let unknown = service
            .snapshot("q-unknown".into())
            .await
            .expect("unknown snapshot should load");
        assert_eq!(
            unknown.queue_position,
            Some(2),
            "runs without admission time must sort after dated equals"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    fn synthetic_hitl_record(root: &Utf8Path, run_id: &str) -> RunRecord {
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, root.join("runs"), None)
            .expect("service should initialize");
        let contract = service
            .load_generator("ask-user")
            .expect("ask-user fixture should load");
        let run_dir = root.join("runs").join(run_id);
        prepare_api_run_directory(&run_dir).expect("run directory should prepare");
        let (events, _) = broadcast::channel(512);
        let record = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: BTreeMap::new(),
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            priority: 0,
            parent_run_id: None,
            preempted: false,
            state: RunStatus::Queued,
            run_dir: run_dir.clone(),
            artifacts: None,
            question: None,
            confirm: None,
            events,
            cancellation: CancellationToken::new(),
            task: Arc::new(Mutex::new(None)),
            queued_at: Some(chrono::Utc::now()),
            owner_id: String::new(),
            ephemeral: false,
        };
        write_run_event(
            &record,
            "run_queued",
            json!({
                "run_id": run_id,
                "generator": "ask-user@0.1.0",
                "generator_path": contract.root,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "answers": {},
                "confirmations": {},
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": 1,
            }),
        )
        .expect("queued event should append");
        record
    }

    fn synthetic_question() -> qcg_api::FormSpec {
        qcg_api::FormSpec {
            id: "q1".into(),
            title: "Question".into(),
            title_i18n: Default::default(),
            fields: vec![],
        }
    }

    #[tokio::test]
    async fn rehydrate_restores_accepted_answers_from_journal() {
        let root = temp_run_dir("hitl-rehydrate");
        let _ = std::fs::remove_dir_all(&root);
        let run_id = format!("synth-{}", uuid::Uuid::now_v7());
        let record = synthetic_hitl_record(&root, &run_id);
        let question = synthetic_question();
        write_run_event(
            &record,
            "run_waiting",
            json!({ "question_id": "q1", "question": question }),
        )
        .expect("waiting event should append");
        let runs_dir = root.join("runs");
        let pending =
            rehydrate_runs(&runs_dir, DEFAULT_MAX_TRACKED_RUNS).expect("rehydrate should succeed");
        let waiting = pending.get(&run_id).expect("run should rehydrate");
        assert_eq!(waiting.state, RunStatus::Waiting);
        assert!(waiting.answers.is_empty());
        write_run_event(
            &record,
            "user_answered",
            json!({ "question_id": "q1", "values": { "answer": "brief" } }),
        )
        .expect("answer event should append");
        let pending =
            rehydrate_runs(&runs_dir, DEFAULT_MAX_TRACKED_RUNS).expect("rehydrate should succeed");
        let queued = pending.get(&run_id).expect("run should rehydrate");
        assert_eq!(queued.state, RunStatus::Queued);
        assert_eq!(
            queued.answers.get("q1"),
            Some(&json!({ "answer": "brief" })),
            "accepted answer must survive a restart before the engine consumes it"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn mcp_pending_continuation_roundtrips_through_journal() {
        let root = temp_run_dir("mcp-pending");
        let _ = std::fs::remove_dir_all(&root);
        let run_id = format!("synth-{}", uuid::Uuid::now_v7());
        let record = synthetic_hitl_record(&root, &run_id);
        write_run_event(
            &record,
            "mcp_input_pending",
            json!({
                "node": "fetch",
                "pending_key": "fetch:mcpcont:server/tool:abc123#__mcp_pending",
                "question_id": "fetch:mcp:server/tool:deadbeef",
                "server": "server",
                "tool": "tool",
                "call_id": "call-1",
                "arguments": { "q": "x" },
                "request_state": "state-1",
                "input_requests": {},
            }),
        )
        .expect("pending event should append");
        // The typed journal store (not the answers map) carries the
        // descriptor, including the invocation binding.
        let state = crate::summaries::fold_run_state(&root.join("runs").join(&run_id))
            .expect("journal should fold");
        let descriptor = state
            .mcp_pending
            .get("fetch:mcpcont:server/tool:abc123#__mcp_pending")
            .expect("pending descriptor should be keyed by reservation");
        assert_eq!(
            descriptor.get("request_state").and_then(Value::as_str),
            Some("state-1")
        );
        assert_eq!(
            descriptor.get("question_id").and_then(Value::as_str),
            Some("fetch:mcp:server/tool:deadbeef")
        );
        assert_eq!(
            descriptor.get("call_id").and_then(Value::as_str),
            Some("call-1")
        );
        // Completion consumes the continuation so later identical calls
        // start fresh instead of resuming it.
        write_run_event(
            &record,
            "mcp_continuation_consumed",
            json!({
                "node": "fetch",
                "pending_key": "fetch:mcpcont:server/tool:abc123#__mcp_pending",
            }),
        )
        .expect("consume event should append");
        let state = crate::summaries::fold_run_state(&root.join("runs").join(&run_id))
            .expect("journal should fold");
        assert!(
            !state
                .mcp_pending
                .contains_key("fetch:mcpcont:server/tool:abc123#__mcp_pending"),
            "consumed continuation must leave the store"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn shared_peer_answer_is_adopted_without_duplicate_execution() {
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = temp_run_dir("shared-peer-answer");
        let _ = std::fs::remove_dir_all(&runs);
        let make_service = || {
            LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
            )
            .expect("shared service should initialize")
        };
        let owner = make_service();
        let id = owner
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("interactive run should start");
        let question = wait_for_snapshot(&owner, &id, RunStatus::Waiting)
            .await
            .question
            .expect("run should be waiting");
        // A peer attached to the same store observes the waiting run and
        // answers it; the durable journal carries the decision over.
        let peer = make_service();
        let peer_snapshot = peer
            .snapshot(id.clone())
            .await
            .expect("peer should observe the waiting run");
        assert_eq!(peer_snapshot.state, RunStatus::Waiting);
        peer.answer(
            id.clone(),
            question.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("peer answer should be accepted");
        let terminal = wait_for_terminal_snapshot(&peer, &id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        let journal = read_journal_string(&peer, id.clone()).await;
        assert!(
            journal.contains("\"t\":\"user_answered\""),
            "peer decision must be journaled for the owner to adopt"
        );
        assert_eq!(
            journal.matches("\"t\":\"run_finished\"").count(),
            1,
            "takeover must settle the run exactly once"
        );
        drop(owner);
        drop(peer);
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn concurrent_conflicting_answers_journal_once() {
        let runs = temp_run_dir("answer-race");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("interactive run should start");
        let question = wait_for_snapshot(&service, &id, RunStatus::Waiting)
            .await
            .question
            .expect("run should be waiting");
        let answer = |value: &str| {
            service.answer(
                id.clone(),
                question.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!(value))]),
                },
            )
        };
        let (first, second) = tokio::join!(answer("brief"), answer("detailed"));
        let succeeded = [&first, &second].iter().filter(|r| r.is_ok()).count();
        assert_eq!(succeeded, 1, "exactly one conflicting answer must win");
        for result in [&first, &second] {
            if let Err(error) = result {
                assert!(
                    error.to_string().contains("different values"),
                    "loser must see a conflict, got {error}"
                );
            }
        }
        let journal = read_journal_string(&service, id.clone()).await;
        assert_eq!(
            journal.matches("\"t\":\"user_answered\"").count(),
            1,
            "conflicting answers must not both persist with last-wins"
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn shutdown_converges_when_executor_ignores_cancellation() {
        let root = temp_run_dir("shutdown-deadline");
        let _ = std::fs::remove_dir_all(&root);
        write_generator_package(&root.join("generators"), "queue-gen");
        let service = LocalQcgService::new(root.join("generators"), root.join("runs"), None)
            .expect("service should initialize");
        let contract = service
            .load_generator("queue-gen")
            .expect("fixture generator should load");
        let run_id = "stuck-run";
        let run_dir = root.join("runs").join(run_id);
        prepare_api_run_directory(&run_dir).expect("run directory should prepare");
        let (events, _) = broadcast::channel(512);
        // An executor that never observes cancellation, as with a wedged
        // container runtime client.
        let stuck = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let record = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: BTreeMap::new(),
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            priority: 0,
            parent_run_id: None,
            preempted: false,
            state: RunStatus::Running,
            run_dir: run_dir.clone(),
            artifacts: None,
            question: None,
            confirm: None,
            events,
            cancellation: CancellationToken::new(),
            task: Arc::new(Mutex::new(Some(stuck))),
            queued_at: None,
            owner_id: String::new(),
            ephemeral: false,
        };
        write_run_event(
            &record,
            "run_queued",
            json!({
                "run_id": run_id,
                "generator": "queue-gen@0.1.0",
                "generator_path": contract.root,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": 1,
            }),
        )
        .expect("queued event should append");
        service
            .inner
            .runs
            .write()
            .await
            .insert(run_id.to_string(), record);
        let started = std::time::Instant::now();
        service
            .shutdown_active_runs()
            .await
            .expect("shutdown must converge despite the stuck executor");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(25),
            "shutdown deadline must bound the wait"
        );
        let journal = read_journal_string(&service, run_id.to_string()).await;
        assert!(
            journal.contains("run_interrupted"),
            "abandoned work must be marked interrupted for restart triage"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn non_owned_runs_follow_journal_for_sse() {
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = temp_run_dir("sse-ownership");
        let _ = std::fs::remove_dir_all(&runs);
        let make_service = || {
            LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
            )
            .expect("shared service should initialize")
        };
        let owner = make_service();
        let id = owner
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("interactive run should start");
        wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        // Claim local ownership as if this process drove execution.
        {
            let mut records = owner.inner.runs.write().await;
            let record = records.get_mut(&id).expect("run should be tracked");
            record.owner_id = owner.inner.owner_id.clone();
        }
        assert!(
            owner.owns_live_stream(&id).await,
            "the owning process must keep its live broadcast"
        );
        let peer = make_service();
        assert!(
            !peer.owns_live_stream(&id).await,
            "a non-owning peer must follow the durable journal instead of a stale broadcast"
        );
        // Exclusive stores always own their (single-process) broadcast.
        let exclusive = LocalQcgService::new(generators, runs.clone(), None)
            .expect("exclusive service should initialize");
        assert!(exclusive.owns_live_stream(&id).await);
        drop(owner);
        drop(peer);
        drop(exclusive);
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn start_run_persists_canonical_default_inputs() {
        // A10: admission resolves defaults once and persists the canonical
        // inputs to the journal, the memory record, and the engine.
        let root = temp_run_dir("canonical-inputs");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Canonical Inputs"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "main"

[[inputs.stages.fields]]
id = "name"
type = "string"
default = "world"

[[flow]]
id = "out"
type = "write"

[flow.params]
content = "hello {{ inputs.name }}"
output_file = "out.txt"

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let journal = read_journal_string(&service, id.clone()).await;
        assert!(
            journal.contains("\"name\":\"world\"") || journal.contains("\"name\": \"world\""),
            "canonical default input must reach the journal, got: {journal}"
        );
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run dir should resolve");
        let state = crate::summaries::fold_run_state(&run_dir).expect("fold should succeed");
        assert_eq!(
            state.inputs.as_ref().and_then(|inputs| inputs.get("name")),
            Some(&json!("world")),
            "folded inputs must carry the resolved default"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn cancel_mailbox_reaches_disk_only_peer_owner() {
        // A02: a peer tracking no local record still delivers cancellation
        // through the durable mailbox.
        let root = temp_run_dir("cancel-mailbox");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Cancel Mailbox"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::new(root.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        // Simulate a peer process sharing the runs directory but tracking
        // nothing in memory: dropping its memory record must not drop the
        // durable cancel signal.
        let peer = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("peer should initialize");
        peer.inner.runs.write().await.remove(&id);
        peer.cancel(id.clone())
            .await
            .expect("disk-only peer cancel should succeed");
        let run_dir = owner
            .run_dir_for(&id)
            .await
            .expect("run dir should resolve");
        assert!(
            crate::summaries::has_remote_cancel_request(&run_dir)
                .expect("cancel check should succeed"),
            "mailbox cancel must be observable by the owner"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn cancel_acceptance_is_not_settlement_until_terminal_is_journaled() {
        // A02: a mailbox cancel observed by a non-owning peer reports
        // `CancelRequested`, never `Canceled`. Only a journaled terminal
        // outcome settles the display.
        let root = temp_run_dir("cancel-acceptance");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Cancel Acceptance"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::new(root.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        // A non-owning peer shares the directory but tracks nothing.
        let peer = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("peer should initialize");
        let run_dir = peer.run_dir_for(&id).await.expect("run dir should resolve");
        crate::run_dirs::request_remote_cancel(&run_dir, &id, "peer-test")
            .expect("mailbox write should succeed");
        // No terminal outcome exists: nothing settled.
        let folded = crate::summaries::fold_run_state(&run_dir).expect("fold should succeed");
        assert!(
            folded.terminal.is_none(),
            "mailbox acceptance must not journal a terminal outcome by itself"
        );
        peer.refresh_shared_runs()
            .await
            .expect("refresh should succeed");
        assert_eq!(
            peer.snapshot(id.clone())
                .await
                .expect("snapshot should be available")
                .state,
            RunStatus::CancelRequested,
            "mailbox acceptance must display as requested, not canceled"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn accepted_cancel_converges_to_terminal_without_a_second_cancel() {
        // B10: continuing the acceptance scenario, recovery adopts the
        // task-less accepted run and the common finalizer journals the
        // terminal outcome. No second cancel is ever issued.
        let root = temp_run_dir("cancel-converge");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Cancel Converge"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::new(root.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        let peer = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("peer should initialize");
        let run_dir = peer.run_dir_for(&id).await.expect("run dir should resolve");
        crate::run_dirs::request_remote_cancel(&run_dir, &id, "peer-test")
            .expect("mailbox write should succeed");
        peer.refresh_shared_runs()
            .await
            .expect("refresh should succeed");
        assert_eq!(
            peer.snapshot(id.clone())
                .await
                .expect("snapshot should be available")
                .state,
            RunStatus::CancelRequested,
            "acceptance must display before settlement"
        );
        // Recovery adopts the accepted run; the spawned task settles it.
        peer.resume_recovered_runs().await;
        let terminal = wait_for_terminal_snapshot(&peer, &id).await;
        assert_eq!(
            terminal.state,
            RunStatus::Canceled,
            "accepted cancel must converge to canceled"
        );
        // Exactly one terminal outcome is journaled: acceptance never
        // counted as settlement, settlement never duplicated it.
        let folded = crate::summaries::fold_run_state(&run_dir).expect("fold should succeed");
        assert!(
            folded.terminal.is_some(),
            "convergence must journal a terminal outcome"
        );
        let events = crate::summaries::read_events_from_meta(&crate::run_meta_dir(&run_dir))
            .expect("events should read");
        let terminals = events
            .iter()
            .filter(|event| {
                matches!(
                    event.get("t").and_then(serde_json::Value::as_str),
                    Some("run_canceled")
                        | Some("run_finished")
                        | Some("run_error")
                        | Some("run_interrupted")
                )
            })
            .count();
        assert_eq!(terminals, 1, "exactly one terminal event must exist");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn losing_hitl_race_answers_without_deadlocking_the_run_map() {
        // B01: a peer that loses the journal precondition race must report
        // the rejection promptly instead of deadlocking the runs map against
        // itself (write guard held across a lock-taking classify await).
        // Two services share one runs directory so neither peer's memory
        // fast path can decide the race; only the durable precondition can.
        let root = temp_run_dir("hitl-race-no-deadlock");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Race"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes", "no"]

[permissions]
fs_write = ["workspace"]
"#,
        )
        .expect("generator manifest should be written");
        let make_service = || {
            LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
                vec![root.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
            )
            .expect("shared service should initialize")
        };
        let owner = make_service();
        let peer = make_service();
        let id = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let question = wait_for_snapshot(&owner, &id, RunStatus::Waiting)
            .await
            .question
            .expect("run should be waiting");
        // A second run proves the map stays responsive while the loser is
        // being classified.
        let probe = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("probe run should start");
        // The peer observes the waiting generation before the owner wins,
        // so its memory checks pass later and only the journal precondition
        // can reject it.
        peer.refresh_shared_runs()
            .await
            .expect("refresh should succeed");
        owner
            .answer(
                id.clone(),
                question.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("winning answer should be accepted");
        let (loser, probe_snapshot) = tokio::join!(
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                peer.answer(
                    id.clone(),
                    question.id.clone(),
                    AnswerPayload {
                        values: BTreeMap::from([("answer".into(), json!("no"))]),
                    },
                )
            ),
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                peer.snapshot(probe.clone()),
            ),
        );
        let loser = loser
            .expect("losing answer must respond, not deadlock")
            .expect_err("losing answer must be rejected");
        // Either accurate classification proves prompt rejection without
        // deadlock: a finished run reports terminal, a still-settling run
        // reports the conflicting acceptance.
        assert!(
            loser.to_string().contains("different values")
                || loser.to_string().contains("already terminal"),
            "loser should see the conflict, got: {loser}"
        );
        probe_snapshot
            .expect("unrelated snapshot must stay responsive")
            .expect("probe snapshot should exist");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn snapshot_exposes_generator_id_without_parsing_run_id() {
        // C03: snapshots carry the generator id explicitly so UUID hyphens
        // never leak into parsed ids.
        let runs = temp_run_dir("snapshot-generator-id");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators, runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                answers: BTreeMap::from([("choose_mode".into(), json!("brief"))]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.generator_id.as_str(), "ask-user");
        assert!(
            !snapshot.generator_id.contains('-') || snapshot.generator_id == "ask-user",
            "generator id must not contain UUID fragments"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }
}
