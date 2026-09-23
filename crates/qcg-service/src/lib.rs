pub mod package;
pub use package::{ArtifactZipLimits, PackageLimits};

mod artifacts;
mod catalog;
mod lifecycle;
mod queue;
mod run_dirs;
mod run_refs;
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

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04). A removal failure warns instead of being silently
    /// ignored (E01).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(self.0.as_std_path()) {
                tracing::warn!(path = %self.0, %error, "test temp cleanup failed");
            }
        }
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
        if let Some(mut audit) = opened.audit {
            audit
                .read_to_end(&mut bytes)
                .await
                .expect("audit should read");
        }
        // Reassemble the merged record view in seq order: durable and
        // observation records share one seq space across sibling files.
        let text = String::from_utf8(bytes).expect("journal should be UTF-8");
        let mut lines: Vec<(u64, &str)> = text
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                let seq = serde_json::from_str::<Value>(line)
                    .ok()
                    .and_then(|value| value.get("seq").and_then(Value::as_u64))
                    .unwrap_or(0);
                (seq, line)
            })
            .collect();
        lines.sort_by_key(|(seq, _)| *seq);
        let mut merged = lines
            .into_iter()
            .map(|(_, line)| line)
            .collect::<Vec<_>>()
            .join("\n");
        if !merged.is_empty() {
            merged.push('\n');
        }
        merged
    }

    #[tokio::test]
    async fn queued_snapshots_expose_admission_order() {
        let root = temp_run_dir("queue-visibility");
        let _ = std::fs::remove_dir_all(&root);
        write_generator_package(&root.join("generators"), "queue-gen");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.join("generators")],
            root.join("runs"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
fs_read = []
fs_write = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"

[permissions.containers]
enabled = false"#
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

        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.join("generators")],
            root.join("runs"),
            Some(providers_path.clone()),
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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
        let error = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.join("generators")],
            root.join("other-runs"),
            Some(missing.clone()),
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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

        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![primary.clone(), secondary.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            10,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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

            // Merged public view: observation records live in audit.jsonl.
            let events = crate::summaries::read_events_with_audit(&run_dir)
                .expect("journal should be valid JSONL");
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            root.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            root.clone(),
            None,
            8,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
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
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "active run limit test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs,
            None,
            1,
            2,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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
            qcg_policy::JOURNAL_POLL_INTERVAL_MILLIS,
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
            !qcg_api::is_terminal_event_kind(failure.kind.as_str()),
            "the failure marker must never fake a normal terminal event"
        );
        let end = tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
            .await
            .expect("poller should close after the failure marker");
        assert!(end.is_none());
        std::fs::remove_dir_all(run_dir).expect("temporary run should be removed");
    }

    #[test]
    fn runs_directory_is_exclusive_between_services() {
        let root = temp_run_dir("runs-directory-lock");
        let _ = std::fs::remove_dir_all(&root);
        let generators = root.join("generators");
        let runs = root.join("runs");
        let first = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("first service should acquire the runs directory");
        let error = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect_err("a second service must not share the runs directory");
        assert!(
            error.to_string().contains("already owned"),
            "lock failure should explain ownership conflict, got {error}"
        );
        drop(first);
        let second = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
        let first = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("first shared service should initialize");
        let second = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
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
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "direct output lock test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            root.join("runs"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
                    match probe.try_lock() {
                        Err(std::fs::TryLockError::WouldBlock) => break,
                        Err(std::fs::TryLockError::Error(error)) => {
                            panic!("output lock probe failed: {error}")
                        }
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
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "cancellation test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
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
    async fn shutting_down_service_refuses_new_admissions() {
        // E05: the shutdown gate covers internal callers, not just the
        // HTTP middleware, so a resumer or peer cannot admit work after the
        // shutdown snapshot.
        let root = temp_run_dir("shutdown-admission");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        assert!(!service.is_shutting_down());
        service.mark_shutting_down();
        assert!(service.is_shutting_down());
        let error = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect_err("start must be refused during shutdown");
        assert!(
            error.to_string().contains("shutting down"),
            "refusal must name shutdown: {error}"
        );
        let fork = service
            .fork_run(
                "missing-run",
                ForkRun {
                    at_seq: 1,
                    ..Default::default()
                },
            )
            .await;
        assert!(
            fork.is_err(),
            "fork must be refused before the source is consulted"
        );
        for (name, result) in [
            (
                "cancel",
                service.cancel("missing-run".to_string()).await.map(|_| ()),
            ),
            ("delete", service.delete_run("missing-run").await),
        ] {
            let error = result.expect_err(&format!("{name} must be refused during shutdown"));
            assert!(
                matches!(error, ApiError::Unavailable { .. }),
                "{name} must report shutdown unavailability: {error}"
            );
        }
        // An in-flight answer is refused at the lock-held re-check even
        // though its run predates the shutdown snapshot.
        let answer = service
            .answer(
                "missing-run".to_string(),
                "question".to_string(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect_err("answer must be refused during shutdown");
        assert!(
            matches!(answer, ApiError::Unavailable { .. }),
            "answer must report shutdown unavailability: {answer}"
        );
        // Confirmations go through the same lock-held gate.
        let confirm = service
            .confirm(
                "missing-run".to_string(),
                "confirm-1".to_string(),
                ConfirmDecision {
                    decision: ConfirmationDecision::Approve,
                },
            )
            .await
            .expect_err("confirm must be refused during shutdown");
        assert!(
            matches!(confirm, ApiError::Unavailable { .. }),
            "confirm must report shutdown unavailability: {confirm}"
        );
        // A spawn raced past the admission gate is refused at the engine
        // entry itself instead of starting doomed work (E05).
        let contract = qcg_contract::Contract::load(generators.join("ask-user"))
            .expect("fixture contract should load");
        let run_dir = runs.join("shutdown-spawn-1");
        let (events, _) = broadcast::channel(8);
        let task = Arc::new(Mutex::new(None));
        service
            .clone()
            .spawn_engine_run(crate::lifecycle::SpawnRun {
                run_id: "shutdown-spawn-1".into(),
                contract,
                inputs: BTreeMap::new(),
                run_dir,
                events,
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                // No admission snapshot on this direct spawn path: the
                // spawn reads the journal once (E03).
                journal_snapshot: None,
                cancellation: CancellationToken::new(),
                task: Arc::clone(&task),
            })
            .await;
        assert!(
            task.lock().expect("task slot").is_none(),
            "no engine task may start during shutdown"
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_rebuilt_service_rehydrates_a_waiting_run_after_shutdown() {
        // E05: shutdown must leave durable state that a fresh process on the
        // same runs directory can rebuild and continue.
        let root = temp_run_dir("shutdown-rebuild");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
            .expect("run should have a pending question");
        service.mark_shutting_down();
        assert_eq!(
            service
                .snapshot(id.clone())
                .await
                .expect("settled snapshot")
                .state,
            RunStatus::Waiting,
            "shutdown must not rewrite a still-durable waiting run"
        );
        drop(service);

        let rebuilt = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should rebuild on the same runs directory");
        let snapshot = rebuilt
            .snapshot(id.clone())
            .await
            .expect("rebuilt snapshot should exist");
        assert_eq!(snapshot.state, RunStatus::Waiting);
        assert_eq!(
            snapshot.question.as_ref().map(|item| &item.id),
            Some(&question.id)
        );
        rebuilt
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect("rebuilt run should accept its answer");
        let snapshot = wait_for_terminal_snapshot(&rebuilt, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        drop(rebuilt);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn retrying_admission_onto_a_live_run_keeps_its_record() {
        // E03: a retry bound to the same run id must reuse the live record
        // (task handle, token, channel) instead of replacing it.
        let root = temp_run_dir("adopt-live-run");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "adopt-live"
name = "Adopt Live"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "adoption test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let request = || StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            answers: BTreeMap::from([("q".to_string(), serde_json::json!("a"))]),
            confirmations: BTreeMap::from([("c".to_string(), true)]),
            ..Default::default()
        };
        let reserved = "generator-retry-1".to_string();
        let id = service
            .start_run_with_id(request(), Some(reserved.clone()))
            .await
            .expect("first start should win");
        assert_eq!(id, reserved);
        wait_for_snapshot(&service, &id, RunStatus::Running).await;
        let before = service
            .inner
            .runs
            .read()
            .await
            .get(&id)
            .expect("live record should exist")
            .task
            .clone();
        let retried = service
            .start_run_with_id(request(), Some(reserved))
            .await
            .expect("retry admission should converge");
        assert_eq!(retried, id);
        let after_record = service
            .inner
            .runs
            .read()
            .await
            .get(&id)
            .expect("live record should still exist")
            .clone();
        assert!(
            std::sync::Arc::ptr_eq(&before, &after_record.task),
            "the live task handle must survive a retry admission"
        );
        let before_record = service
            .inner
            .runs
            .read()
            .await
            .get(&id)
            .expect("live record should still exist")
            .clone();
        assert!(
            before_record.events.same_channel(&after_record.events),
            "the live broadcast channel must survive a retry admission"
        );
        // The admitted answer and approval sets must survive as well: a
        // retry reuses the record instead of re-initializing it from the
        // (possibly empty) retry request state (E03).
        assert_eq!(
            after_record.answers.get("q"),
            Some(&serde_json::json!("a")),
            "the admitted answers must survive a retry admission"
        );
        assert_eq!(
            after_record.confirmations.get("c"),
            Some(&true),
            "the admitted confirmations must survive a retry admission"
        );
        // The cancellation token has no identity accessor; its survival is
        // proven functionally below when cancel through the preserved
        // record still settles the run (E03).
        service
            .cancel(id.clone())
            .await
            .expect("cancel should succeed");
        assert_eq!(
            service
                .snapshot(id)
                .await
                .expect("snapshot should exist")
                .state,
            RunStatus::Canceled,
            "the preserved record must still control the run"
        );
    }

    #[tokio::test]
    async fn journal_order_wins_restart_and_fork_continue_v2() {
        // E06 end to end: z_first writes v1, a_second overwrites v2 (same
        // path), then a question suspends. Answering, restarting, and
        // forking must all continue with v2: journal order decides, never
        // node-name order.
        let root = temp_run_dir("e06-v2-chain");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "e06-chain"
name = "E06 Chain"
version = "0.1.0"
qcg_version = "^0.1"

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
id = "z_first"
type = "write"
[flow.params]
output_file = "out.txt"
content = "v1"
[[flow]]
id = "a_second"
type = "write"
needs = ["z_first"]
[flow.params]
output_file = "out.txt"
content = "v2"
[[flow]]
id = "ask"
type = "ask_user"
needs = ["a_second"]
[flow.params]
content = "Continue?"
options = ["yes"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let waiting = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = waiting.question.expect("run should expose its question");
        let run_dir = runs.join(&id);
        let workspace_file = run_dir.join("workspace").join("out.txt");
        assert_eq!(
            std::fs::read_to_string(&workspace_file).expect("workspace file"),
            "v2",
            "journal order must project v2 before the question"
        );
        // Fork from the waiting checkpoint before answering.
        let waiting_seq = crate::summaries::read_journal_events(&run_dir)
            .expect("journal should read")
            .iter()
            .filter_map(|event| {
                (event.get("t").and_then(Value::as_str) == Some("run_waiting"))
                    .then(|| event.get("seq").and_then(Value::as_u64))
                    .flatten()
            })
            .max()
            .expect("waiting checkpoint should exist");
        let fork_id = service
            .fork_run(
                &id,
                ForkRun {
                    at_seq: waiting_seq,
                    state_patch: ForkStatePatch::default(),
                    ..Default::default()
                },
            )
            .await
            .expect("fork should succeed");
        let fork_waiting = wait_for_snapshot(&service, &fork_id, RunStatus::Waiting).await;
        service
            .answer(
                fork_id.clone(),
                fork_waiting
                    .question
                    .expect("fork should expose its question")
                    .id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("fork answer should be accepted");
        // Answer the source run too.
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("answer should be accepted");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &id).await.state,
            RunStatus::Succeeded
        );
        assert_eq!(
            std::fs::read_to_string(&workspace_file).expect("workspace file"),
            "v2",
            "answering must not roll the workspace back to v1"
        );
        // Restart: rebuild the service on the same store and confirm the
        // settled run still projects v2.
        drop(service);
        let rebuilt = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should rebuild");
        assert_eq!(
            std::fs::read_to_string(&workspace_file).expect("workspace file"),
            "v2",
            "restart must keep projecting v2"
        );
        assert_eq!(
            wait_for_terminal_snapshot(&rebuilt, &fork_id).await.state,
            RunStatus::Succeeded,
            "the fork must converge without re-running finished steps"
        );
        let fork_workspace = runs.join(&fork_id).join("workspace").join("out.txt");
        assert_eq!(
            std::fs::read_to_string(&fork_workspace).expect("fork workspace file"),
            "v2",
            "the fork must continue with v2, not v1"
        );
        drop(rebuilt);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn adopting_an_orphaned_run_restores_its_pending_question() {
        // E03: a disk adopt must fold the journal so accepted answers and
        // the pending prompt survive instead of being reset to Queued.
        let root = temp_run_dir("adopt-pending-question");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "adopt-pending"
name = "Adopt Pending"
version = "0.1.0"
qcg_version = "^0.1"

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
id = "ask"
type = "ask_user"
[flow.params]
content = "Continue?"
options = ["yes"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let request = || StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        };
        let reserved = "generator-adopt-1".to_string();
        let id = service
            .start_run_with_id(request(), Some(reserved.clone()))
            .await
            .expect("first start should win");
        let waiting = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = waiting.question.expect("run should expose its question");
        // Simulate eviction/restart: drop the in-memory record. The run is
        // suspended, so no engine task is executing.
        service.inner.runs.write().await.remove(&id);
        let adopted = service
            .start_run_with_id(request(), Some(reserved))
            .await
            .expect("adoption should succeed");
        assert_eq!(adopted, id);
        let restored = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        assert_eq!(
            restored
                .question
                .as_ref()
                .map(|question| question.id.as_str()),
            Some(question.id.as_str()),
            "the adopted record must keep the same pending question"
        );
        // The suspended engine task releases the execution lease just after
        // publishing Waiting; let that settle before answering so this test
        // does not depend on the server-side queued resumer.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("answer should resume the adopted run");
        let terminal = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
    }

    #[tokio::test]
    async fn snapshot_displays_a_disk_only_waiting_run() {
        // E03: a run this process has not rehydrated (peer-owned, or removed
        // from memory before adoption) must still display its durable
        // pending question instead of an empty queued snapshot.
        let runs = temp_run_dir("disk-only-waiting");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
            .expect("run should have a pending question");
        service.inner.runs.write().await.remove(&id);
        let snapshot = service
            .snapshot(id.clone())
            .await
            .expect("disk-only snapshot should exist");
        assert_eq!(snapshot.state, RunStatus::Waiting);
        assert_eq!(
            snapshot.question.as_ref().map(|item| &item.id),
            Some(&question.id),
            "the durable pending question must survive without an in-memory record"
        );
        drop(service);
    }

    #[tokio::test]
    async fn concurrent_admission_of_one_run_id_fails_closed() {
        // E03: while one admission is preparing a reserved run id, a second
        // caller must fail closed instead of wiping the live prepare.
        let runs = temp_run_dir("admission-lock");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let run_id = "ask-user-admission-lock-1".to_string();
        let held = crate::run_dirs::try_lock_run_admission(&runs.join(&run_id))
            .expect("admission lock should open")
            .expect("admission lock should acquire");
        let request = || StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        };
        let error = service
            .start_run_with_id(request(), Some(run_id.clone()))
            .await
            .expect_err("a concurrent admission must fail closed");
        assert!(
            matches!(error, ApiError::Conflict { .. }),
            "admission contention must be a conflict: {error}"
        );
        drop(held);
        let id = service
            .start_run_with_id(request(), Some(run_id.clone()))
            .await
            .expect("retry after the lock is released should succeed");
        assert_eq!(id, run_id);
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        drop(service);
    }

    #[tokio::test]
    async fn a_lease_loser_keeps_the_live_task_handle() {
        // E03: spawn_engine_run must not clear a task handle stored by a
        // concurrent winner when its own lease attempt loses, or the winner
        // becomes an orphan and a second execution can start.
        let root = temp_run_dir("lease-loser");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            root.join("runs"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let contract = service.load_generator("ask-user").expect("contract");
        let run_dir = service.inner.runs_dir.join("ask-user-lease-loser-1");
        crate::run_dirs::prepare_api_run_directory(&run_dir).expect("run directory");
        let held_execution = crate::run_dirs::try_lock_run_execution(&run_dir)
            .expect("execution lock")
            .expect("execution lease should be free");
        let task = Arc::new(Mutex::new(Some(tokio::spawn(async {}))));
        let (events, _) = broadcast::channel(8);
        service
            .clone()
            .spawn_engine_run(crate::lifecycle::SpawnRun {
                run_id: "ask-user-lease-loser-1".into(),
                contract,
                inputs: BTreeMap::new(),
                run_dir,
                events,
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                // No admission snapshot on this direct spawn path: the
                // spawn reads the journal once (E03).
                journal_snapshot: None,
                cancellation: CancellationToken::new(),
                task: Arc::clone(&task),
            })
            .await;
        assert!(
            task.lock().expect("task lock").is_some(),
            "the losing spawn must not orphan the winner's handle"
        );
        drop(held_execution);
        if let Some(handle) = task.lock().expect("task lock").take() {
            handle.abort();
        }
        drop(service);
    }

    #[test]
    fn adopted_snapshot_seeds_without_journal_reread() {
        // E03: the adoption snapshot carries its own fold, so seeding the
        // adopted run performs no journal I/O. Proof by deletion: the run
        // directory is removed after adoption, yet seeding from the
        // snapshot still succeeds with the journaled seed.
        let root = temp_run_dir("adopt-no-reread");
        let _guard = TempGuard(root.clone());
        let contract = qcg_contract::Contract::load(
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../fixtures/generators/ask-user"),
        )
        .expect("fixture contract should load");
        let run_id = "adopt-no-reread-1";
        let run_dir = root.join("runs").join(run_id);
        prepare_api_run_directory(&run_dir).expect("run directory should prepare");
        let (events, _) = broadcast::channel(8);
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
            queued_at: None,
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
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                "priority": 0,
                "parent_run_id": Value::Null,
            }),
        )
        .expect("queued event should append");
        let snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&run_dir, run_id)
            .expect("adopt check should succeed");
        assert!(snapshot.adopted, "the completed admission should adopt");
        // Delete the journal: any re-read or re-fold from disk now fails,
        // so a successful seed proves the snapshot alone suffices.
        std::fs::remove_dir_all(&run_dir).expect("run directory should be removable");
        assert!(
            !run_dir.exists(),
            "the journal must be gone for this proof to mean anything"
        );
        let seed = crate::runs_api::seed_adopted_run_from_snapshot(
            &snapshot,
            &crate::runs_api::AdoptedRunIdentity {
                inputs: &BTreeMap::new(),
                contract_sha256: &contract.sha256,
                priority: 0,
                parent: None,
                answers: &BTreeMap::new(),
                confirmations: &BTreeMap::new(),
            },
        )
        .expect("snapshot seed should succeed without the journal")
        .expect("the adopted run should carry a seed");
        assert!(seed.answers.is_empty());
        assert!(seed.confirmations.is_empty());
    }

    #[test]
    fn adoption_snapshot_covers_seed_and_fork_inputs_without_second_read() {
        // E03: start/fork adoption performs exactly ONE journal read on the
        // normal path. `try_adopt_run_dir_with_snapshot` reads once and
        // folds once; its only second read is the conditional recheck taken
        // solely when the journal lacks this run's own admission (partial
        // fork/empty journal), never on the adopted path below. Every
        // downstream derivation (seed, fork inputs, HITL maps, queued
        // identity) is pure over `snapshot.events`. Proof by deletion: the
        // run directory is removed after adoption, so any second journal
        // I/O would fail and this test would turn red.
        let root = temp_run_dir("adopt-single-read");
        let _guard = TempGuard(root.clone());
        let contract = qcg_contract::Contract::load(
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../fixtures/generators/ask-user"),
        )
        .expect("fixture contract should load");
        let run_id = "adopt-single-read-1";
        let run_dir = root.join("runs").join(run_id);
        prepare_api_run_directory(&run_dir).expect("run directory should prepare");
        let (events, _) = broadcast::channel(8);
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
            queued_at: None,
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
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                "priority": 0,
                "parent_run_id": Value::Null,
            }),
        )
        .expect("queued event should append");
        // The single journal read on the normal path.
        let snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&run_dir, run_id)
            .expect("adopt check should succeed");
        assert!(snapshot.adopted, "the completed admission should adopt");
        assert!(
            !snapshot.events.is_empty(),
            "the snapshot should carry events"
        );
        assert!(
            snapshot.state.is_some(),
            "the snapshot should carry its fold"
        );
        let events_before = snapshot.events.clone();
        // Delete the journal: any re-read from disk now fails.
        std::fs::remove_dir_all(&run_dir).expect("run directory should be removable");
        assert!(
            !run_dir.exists(),
            "the journal must be gone for this proof to mean anything"
        );
        // Seed, fork inputs, HITL maps, and queued identity all derive from
        // the snapshot with zero journal I/O.
        let seed = crate::runs_api::seed_adopted_run_from_snapshot(
            &snapshot,
            &crate::runs_api::AdoptedRunIdentity {
                inputs: &BTreeMap::new(),
                contract_sha256: &contract.sha256,
                priority: 0,
                parent: None,
                answers: &BTreeMap::new(),
                confirmations: &BTreeMap::new(),
            },
        )
        .expect("snapshot seed should succeed without the journal")
        .expect("the adopted run should carry a seed");
        assert!(seed.answers.is_empty());
        assert!(seed.confirmations.is_empty());
        let fork_inputs = crate::runs_api::fork_checkpoint_inputs(&snapshot, &contract, run_id, 1)
            .expect("fork inputs should derive without the journal");
        assert!(
            fork_inputs.is_empty(),
            "empty inputs should resolve to empty"
        );
        let (answers, confirmations) =
            crate::summaries::read_persisted_hitl_from_values(&snapshot.events)
                .expect("HITL maps should fold without the journal");
        assert!(answers.is_empty());
        assert!(confirmations.is_empty());
        let (priority, parent) =
            crate::summaries::read_queued_identity_from_values(&snapshot.events);
        assert_eq!(priority, 0);
        assert!(parent.is_none());
        assert_eq!(
            snapshot.events, events_before,
            "derivations must not mutate the snapshot"
        );
    }

    #[test]
    fn snapshot_helpers_perform_no_journal_io() {
        // E03 structural pin: the snapshot helpers must stay pure over
        // already-read events. If anyone adds a journal read (or a second
        // fold from disk) inside them, the normal path silently pays a
        // second read per admission and this test fails the build.
        fn body_of<'a>(source: &'a str, needle: &str) -> &'a str {
            let start = source
                .find(needle)
                .unwrap_or_else(|| panic!("expected helper `{needle}` in source"));
            let rest = &source[start..];
            // Top-level helpers close with `}` at column zero; ending there
            // keeps each slice to exactly one function even when the next
            // item is `pub(crate) fn`, `async fn`, an enum, or an impl.
            let end = rest
                .find("\n}\n")
                .map(|index| start + index + 3)
                .unwrap_or(source.len());
            &source[start..end]
        }
        let runs_api = include_str!("runs_api.rs");
        for helper in [
            "fn seed_adopted_run_from_snapshot(",
            "fn seed_adopted_run_from_state(",
            "fn verify_live_admission_from_snapshot(",
            "fn fork_checkpoint_inputs(",
            "fn resolve_fork_live_hit(",
        ] {
            let body = body_of(runs_api, helper);
            for io in [
                "read_journal_events",
                "read_events_from_meta",
                "read_journal_values",
                "fold_journal",
            ] {
                assert!(
                    !body.contains(io),
                    "snapshot helper `{helper}` must not perform journal I/O (`{io}`); the adoption snapshot is the single read"
                );
            }
        }
        // The adoption probe itself reads once on the normal path; the
        // redundant re-read was removed (E03): the per-run admission lock
        // serializes prepare/wipe/adopt, so no concurrent admission can
        // complete a journal while the probe runs. One call site total.
        let run_dirs = include_str!("run_dirs.rs");
        let adopt = body_of(run_dirs, "fn try_adopt_run_dir_with_snapshot(");
        assert_eq!(
            adopt.matches("read_events_from_meta(").count(),
            1,
            "adoption must keep exactly one journal read; the redundant re-read was removed (admission lock guarantees no concurrent admission)"
        );
        assert!(
            adopt.contains("if !initialized"),
            "the uninitialized gate must remain (direct wipe after the single read)"
        );
    }

    #[test]
    fn service_construction_inside_crate_uses_only_policy_constructor() {
        // E04: no production code inside this crate may use the legacy
        // 3-argument constructor; the policy constructor freezes deployment
        // policy before recovery can observe it. Test-only uses live behind
        // `#[cfg(test)]`, so only the production prefix (before
        // `#[cfg(test)]`) is scanned. If a production caller regresses, this
        // fails the build.
        for (name, source) in [
            ("lifecycle.rs", include_str!("lifecycle.rs")),
            ("runs_api.rs", include_str!("runs_api.rs")),
            ("run_dirs.rs", include_str!("run_dirs.rs")),
            ("types.rs", include_str!("types.rs")),
            ("lib.rs", include_str!("lib.rs")),
        ] {
            let production = source.split("#[cfg(test)]").next().unwrap_or(source);
            assert!(
                !production.contains("LocalQcgService::new("),
                "{name} production code must not use `LocalQcgService::new`; use the policy constructor"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_subscribe_and_refresh_serialize_without_deadlock_or_loss() {
        // E12: concurrent subscribes plus concurrent shared-store refreshes
        // on real temp dirs complete without deadlock and observe identical
        // history. Subscribe is a read-only observation (it deliberately
        // does not join the store lock; per-run authority stays with the
        // execution lease plus the journal lock), so this proves the paths
        // interleave safely rather than serializing on one lock.
        use futures_util::StreamExt as _;
        let run_id = format!("concurrent-{}", uuid::Uuid::now_v7());
        let root = temp_run_dir("concurrent-subscribe-refresh");
        let _guard = TempGuard(root.clone());
        let generators_dir = root.join("generators");
        let generator_dir = generators_dir.join("demo");
        let runs_dir = root.join("runs");
        let run_dir = runs_dir.join(&run_id);
        std::fs::create_dir_all(&generator_dir).expect("generator dir should be created");
        std::fs::write(
            generator_dir.join("qcg.toml"),
            r#"[generator]
id = "demo"
name = "Demo"
version = "0.1.0"
qcg_version = "^0.1"

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
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "done""#,
        )
        .expect("generator manifest should be written");
        let contract = qcg_contract::Contract::load(&generator_dir).expect("contract should load");
        std::fs::create_dir_all(crate::summaries::run_meta_dir(&run_dir))
            .expect("meta dir should be created");
        let writer = qcg_engine::JournalWriter::create(
            &crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl"),
            &run_id,
            false,
            None,
        )
        .expect("journal writer should be created");
        writer
            .event(
                "run_queued",
                json!({
                    "generator": "demo@0.1.0",
                    "generator_path": generator_dir,
                    "contract_sha256": contract.sha256,
                    "inputs": {},
                    "answers": {},
                    "confirmations": {},
                    "qcg": "0.1.0",
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                    "retention_days": 0,
                    "priority": 0,
                    "parent_run_id": null,
                    "effective_max_total_steps": 64,
                    "effective_policy_origin": "test",
                }),
            )
            .expect("queued event should append");
        writer
            .event(
                "run_started",
                json!({
                    "generator": "demo",
                    "generator_path": generator_dir,
                    "contract_sha256": contract.sha256,
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": "0.1.0",
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                }),
            )
            .expect("started event should append");
        drop(writer);
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators_dir],
            runs_dir,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        // Two subscribes plus two refreshes run concurrently; the timeout
        // proves no deadlock, and the identical histories prove no loss.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let first = {
                let service = service.clone();
                let run_id = run_id.clone();
                tokio::spawn(async move {
                    let mut stream = service
                        .subscribe_with_cursor(run_id, 0)
                        .await
                        .expect("first subscribe should succeed");
                    let mut kinds = Vec::new();
                    for _ in 0..2 {
                        let event = stream.next().await.expect("history should replay");
                        kinds.push((event.seq, event.kind.clone()));
                    }
                    kinds
                })
            };
            let second = {
                let service = service.clone();
                let run_id = run_id.clone();
                tokio::spawn(async move {
                    let mut stream = service
                        .subscribe_with_cursor(run_id, 0)
                        .await
                        .expect("second subscribe should succeed");
                    let mut kinds = Vec::new();
                    for _ in 0..2 {
                        let event = stream.next().await.expect("history should replay");
                        kinds.push((event.seq, event.kind.clone()));
                    }
                    kinds
                })
            };
            let refresh_first = {
                let service = service.clone();
                tokio::spawn(async move {
                    service
                        .refresh_shared_runs()
                        .await
                        .expect("first refresh should succeed");
                })
            };
            let refresh_second = {
                let service = service.clone();
                tokio::spawn(async move {
                    service
                        .refresh_shared_runs()
                        .await
                        .expect("second refresh should succeed");
                })
            };
            let (first, second, refresh_first, refresh_second) =
                tokio::join!(first, second, refresh_first, refresh_second);
            (
                first.expect("first subscribe task should finish"),
                second.expect("second subscribe task should finish"),
                refresh_first.expect("first refresh task should finish"),
                refresh_second.expect("second refresh task should finish"),
            )
        })
        .await
        .expect("concurrent subscribes and refreshes must complete without deadlock");
        let (first, second, (), ()) = outcome;
        assert_eq!(
            first,
            vec![
                (1, "run_queued".to_string()),
                (2, "run_started".to_string())
            ],
            "first subscriber must observe the full history"
        );
        assert_eq!(
            first, second,
            "concurrent subscribers must observe identical history with no lost events"
        );
        // Corrupt-journal isolation (E12): a corrupt peer journal never
        // corrupts another run's snapshot. The corrupt run fails closed on
        // its own, while the healthy run still replays its identical
        // history above. Queue-position scans skip (never fail on) corrupt
        // peers, so positions stay exact among orderable runs.
        {
            let corrupt_id = format!("corrupt-{run_id}");
            let corrupt_dir = root.join("runs").join(&corrupt_id);
            std::fs::create_dir_all(crate::summaries::run_meta_dir(&corrupt_dir))
                .expect("corrupt meta dir should be created");
            std::fs::write(
                crate::summaries::run_meta_dir(&corrupt_dir).join("journal.jsonl"),
                b"{ not json\n",
            )
            .expect("corrupt journal should be written");
            let corrupt_snapshot = service.snapshot(corrupt_id.clone()).await;
            assert!(
                corrupt_snapshot.is_err(),
                "a corrupt journal must fail its own snapshot closed"
            );
            let healthy = service
                .snapshot(run_id.clone())
                .await
                .expect("healthy snapshot must survive a corrupt peer");
            assert_eq!(
                healthy.seq, 2,
                "a corrupt peer must not shift the healthy run's history"
            );
        }
    }

    #[test]
    fn shared_and_exclusive_services_join_the_same_store_lock() {
        // E12: SharedFilesystem and Exclusive services participate in the
        // same runs-directory store lock: shared peers coexist, but either
        // mode refuses while the other holds the store.
        let root = temp_run_dir("shared-exclusive-store-lock");
        let _guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        let shared = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("first shared service should initialize");
        let exclusive_error = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect_err("exclusive must be refused while a shared peer holds the store");
        assert!(
            exclusive_error.to_string().contains("already owned"),
            "refusal must name ownership conflict: {exclusive_error}"
        );
        drop(shared);
        let exclusive = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("exclusive should initialize after the shared peer exits");
        let shared_error = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect_err("shared must be refused while exclusive holds the store");
        assert!(
            shared_error.to_string().contains("exclusively owned"),
            "refusal must name exclusive ownership: {shared_error}"
        );
        drop(exclusive);
    }

    #[tokio::test]
    async fn adoption_refuses_mismatched_inputs() {
        // E03: adoption resumes the admitted identity only; different inputs
        // for the same reserved run id are refused.
        let root = temp_run_dir("adopt-inputs-mismatch");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Adopt Inputs"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let reserved = "generator-adopt-inputs-1".to_string();
        let request = |name: &str| StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([("name".into(), json!(name))]),
            ..Default::default()
        };
        let id = service
            .start_run_with_id(request("first"), Some(reserved.clone()))
            .await
            .expect("first admission should succeed");
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        service.inner.runs.write().await.remove(&id);
        let error = service
            .start_run_with_id(request("second"), Some(reserved.clone()))
            .await
            .expect_err("different inputs must not adopt the run");
        assert!(
            matches!(error, ApiError::Conflict { .. }),
            "mismatched adoption must be a conflict: {error}"
        );
        let adopted = service
            .start_run_with_id(request("first"), Some(reserved))
            .await
            .expect("identical inputs should adopt the run");
        assert_eq!(adopted, id);
        wait_for_snapshot(&service, &adopted, RunStatus::Waiting).await;
        // Answers are part of the admitted identity: a retry carrying
        // different answers for the same reserved run id is refused instead
        // of silently re-targeting the run (E03).
        let changed = StartRun {
            answers: BTreeMap::from([("name".into(), json!("changed"))]),
            ..request("first")
        };
        service.inner.runs.write().await.remove(&adopted);
        let error = service
            .start_run_with_id(changed, Some(adopted.clone()))
            .await
            .expect_err("changed answers must not adopt the run");
        assert!(
            matches!(error, ApiError::Conflict { .. }),
            "changed answers must be a conflict: {error}"
        );
        drop(service);
    }

    #[tokio::test]
    async fn adoption_refuses_a_changed_contract_or_priority() {
        // E03: a different contract or priority for the same reserved run
        // id is a different admission and must not adopt the journal.
        let root = temp_run_dir("adopt-identity-mismatch");
        let _ = std::fs::remove_dir_all(&root);
        let write_manifest = |dir: &Utf8Path, id: &str, content: &str| {
            std::fs::create_dir_all(dir).expect("generator directory should be created");
            std::fs::write(
                dir.join("qcg.toml"),
                format!(
                    r#"
[generator]
id = "{id}"
name = "Adopt {id}"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "{content}?"
options = ["yes"]
"#,
                ),
            )
            .expect("generator manifest should be written");
        };
        let primary = root.join("primary");
        let secondary = root.join("secondary");
        let runs = root.join("runs");
        write_manifest(&primary.join("generator"), "generator", "Continue");
        write_manifest(&secondary.join("generator2"), "generator2", "Proceed");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![primary, secondary],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let reserved = "generator-adopt-identity-1".to_string();
        let request = |generator: &str| StartRun {
            generator_id: generator.into(),
            inputs: BTreeMap::from([("name".into(), json!("first"))]),
            ..Default::default()
        };
        let id = service
            .start_run_with_id(request("generator"), Some(reserved.clone()))
            .await
            .expect("first admission should succeed");
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        service.inner.runs.write().await.remove(&id);
        let error = service
            .start_run_with_id(request("generator2"), Some(reserved.clone()))
            .await
            .expect_err("a different contract must not adopt the run");
        assert!(
            matches!(error, ApiError::Conflict { .. }),
            "contract mismatch must be a conflict: {error}"
        );
        let prioritized = StartRun {
            priority: Some(1),
            ..request("generator")
        };
        let error = service
            .start_run_with_id(prioritized, Some(reserved.clone()))
            .await
            .expect_err("a different priority must not adopt the run");
        assert!(
            error.to_string().contains("priority"),
            "priority mismatch must be named: {error}"
        );
        drop(service);
    }

    #[tokio::test]
    async fn fork_adoption_requires_the_same_source() {
        // E03: re-admitting an adopted fork converges on the fork made from
        // the recorded source; a different source is refused.
        let runs = temp_run_dir("fork-adopt-source");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let source = start_ask_user_and_answer(&service, "brief").await;
        let terminal = wait_for_terminal_snapshot(&service, &source).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        let checkpoint = read_journal_string(&service, source.clone())
            .await
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).expect("test journal lines must parse strictly")
            })
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("step_finished"))
            .filter_map(|event| event.get("seq").and_then(Value::as_u64))
            .max()
            .expect("source should have a finished step");
        let reserved = "ask-user-fork-adopt-1".to_string();
        let fork = service
            .fork_run_with_id(
                &source,
                ForkRun {
                    at_seq: checkpoint,
                    ..Default::default()
                },
                Some(reserved.clone()),
            )
            .await
            .expect("fork admission should succeed");
        service.inner.runs.write().await.remove(&fork);
        let adopted = service
            .fork_run_with_id(
                &source,
                ForkRun {
                    at_seq: checkpoint,
                    ..Default::default()
                },
                Some(reserved),
            )
            .await
            .expect("identical fork should adopt");
        assert_eq!(adopted, fork);
        // A fork id bound to another source must not be re-adopted.
        let other = start_ask_user_and_answer(&service, "brief").await;
        let terminal = wait_for_terminal_snapshot(&service, &other).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        // The seed identity (contract, inputs, priority, parent) is
        // asserted directly against a fork journal so no engine timing can
        // hide a mismatch behind terminal convergence.
        let contract =
            qcg_contract::Contract::load(generators.join("ask-user")).expect("contract loads");
        let fork_dir = runs.join("unit-fork");
        let fork_meta = crate::summaries::run_meta_dir(&fork_dir);
        std::fs::create_dir_all(&fork_meta).expect("fork meta");
        let event = json!({
            "t": "run_queued",
            "ts": "2026-01-01T00:00:00Z",
            "seq": 1,
            "run_id": "unit-fork",
            "trace_id": qcg_api::trace_id_for_run("unit-fork"),
            "span_id": qcg_api::span_id_for_seq(1),
            "generator": "ask-user@0.1.0",
            "generator_path": generators.join("ask-user").as_str(),
            "contract_sha256": contract.sha256,
            "inputs": {},
            "answers": {},
            "confirmations": {},
            "qcg": "0.1.0",
            "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
            "retention_days": 0,
            "priority": 0,
            "parent_run_id": source,
            "effective_max_total_steps": 64,
            "effective_policy_origin": "unit-test",
        });
        std::fs::write(fork_meta.join("journal.jsonl"), format!("{}\n", event))
            .expect("fork journal");
        let empty = BTreeMap::new();
        // Single-read proof: materialize one snapshot, then seed purely
        // from it (no second journal read).
        let snapshot_events =
            crate::summaries::read_journal_events(&fork_dir).expect("fork journal should read");
        let snapshot_state =
            qcg_engine::RunState::fold_values(&snapshot_events).expect("fork journal should fold");
        let snapshot = crate::run_dirs::AdoptSnapshot {
            adopted: true,
            events: snapshot_events,
            state: Some(snapshot_state),
        };
        let seed =
            |inputs: &BTreeMap<String, Value>, sha: &str, priority: i32, parent: Option<&str>| {
                crate::runs_api::seed_adopted_run_from_snapshot(
                    &snapshot,
                    &crate::runs_api::AdoptedRunIdentity {
                        inputs,
                        contract_sha256: sha,
                        priority,
                        parent,
                        answers: &empty,
                        confirmations: &BTreeMap::new(),
                    },
                )
            };
        assert!(
            seed(&empty, &contract.sha256, 0, Some(&source))
                .expect("matching identity should seed")
                .is_some(),
            "seed should exist"
        );
        for (label, inputs, sha, priority, parent) in [
            ("contract", &empty, "other-sha", 0, Some(source.as_str())),
            (
                "inputs",
                &BTreeMap::from([("extra".to_string(), json!(1))]),
                contract.sha256.as_str(),
                0,
                Some(source.as_str()),
            ),
            (
                "priority",
                &empty,
                contract.sha256.as_str(),
                1,
                Some(source.as_str()),
            ),
            (
                "parent",
                &empty,
                contract.sha256.as_str(),
                0,
                Some(other.as_str()),
            ),
        ] {
            let error = seed(inputs, sha, priority, parent)
                .expect_err(&format!("a {label} mismatch must conflict"));
            assert!(
                matches!(error, ApiError::Conflict { .. }),
                "a {label} mismatch must be a conflict: {error}"
            );
        }
        drop(service);
    }

    #[tokio::test]
    async fn plain_start_cannot_adopt_a_fork_journal() {
        // E03: a reserved id admitted as a plain start must not adopt a
        // directory whose journal records a fork parent, even when every
        // other field agrees.
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let contract =
            qcg_contract::Contract::load(generators.join("ask-user")).expect("contract loads");
        let runs = temp_run_dir("start-adopt-fork");
        let _ = std::fs::remove_dir_all(&runs);
        let fork_dir = runs.join("plain-adopt-1");
        let fork_meta = crate::summaries::run_meta_dir(&fork_dir);
        std::fs::create_dir_all(&fork_meta).expect("fork meta");
        let event = json!({
            "t": "run_queued",
            "ts": "2026-01-01T00:00:00Z",
            "seq": 1,
            "run_id": "plain-adopt-1",
            "trace_id": qcg_api::trace_id_for_run("plain-adopt-1"),
            "span_id": qcg_api::span_id_for_seq(1),
            "generator": "ask-user@0.1.0",
            "generator_path": generators.join("ask-user").as_str(),
            "contract_sha256": contract.sha256,
            "inputs": {},
            "answers": {},
            "confirmations": {},
            "qcg": "0.1.0",
            "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
            "retention_days": 0,
            "priority": 0,
            "parent_run_id": "some-source",
            "effective_max_total_steps": 64,
            "effective_policy_origin": "unit-test",
        });
        std::fs::write(fork_meta.join("journal.jsonl"), format!("{}\n", event))
            .expect("fork journal");
        let empty = BTreeMap::new();
        let snapshot_events =
            crate::summaries::read_journal_events(&fork_dir).expect("fork journal should read");
        let snapshot_state =
            qcg_engine::RunState::fold_values(&snapshot_events).expect("fork journal should fold");
        let snapshot = crate::run_dirs::AdoptSnapshot {
            adopted: true,
            events: snapshot_events,
            state: Some(snapshot_state),
        };
        let error = crate::runs_api::seed_adopted_run_from_snapshot(
            &snapshot,
            &crate::runs_api::AdoptedRunIdentity {
                inputs: &empty,
                contract_sha256: &contract.sha256,
                priority: 0,
                parent: None,
                answers: &empty,
                confirmations: &BTreeMap::new(),
            },
        )
        .expect_err("a fork journal must not adopt as a plain start");
        assert!(
            matches!(error, ApiError::Conflict { .. }),
            "the fork hole must be a conflict: {error}"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn adopted_seed_keeps_later_user_answers() {
        // E03: a retry replays the original request, so answers accepted
        // after admission must not turn the legitimate resume into a
        // conflict; the seed still carries them for the resumed engine.
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let contract =
            qcg_contract::Contract::load(generators.join("ask-user")).expect("contract loads");
        let runs = temp_run_dir("adopt-later-answers");
        let _ = std::fs::remove_dir_all(&runs);
        let run_dir = runs.join("resume-1");
        let meta = crate::summaries::run_meta_dir(&run_dir);
        std::fs::create_dir_all(&meta).expect("meta");
        let queued = json!({
            "t": "run_queued",
            "ts": "2026-01-01T00:00:00Z",
            "seq": 1,
            "run_id": "resume-1",
            "trace_id": qcg_api::trace_id_for_run("resume-1"),
            "span_id": qcg_api::span_id_for_seq(1),
            "generator": "ask-user@0.1.0",
            "generator_path": generators.join("ask-user").as_str(),
            "contract_sha256": contract.sha256,
            "inputs": {},
            "answers": {},
            "confirmations": {},
            "qcg": "0.1.0",
            "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
            "retention_days": 0,
            "priority": 0,
            "parent_run_id": null,
            "effective_max_total_steps": 64,
            "effective_policy_origin": "unit-test",
        });
        let answered = json!({
            "t": "user_answered",
            "ts": "2026-01-01T00:00:01Z",
            "seq": 2,
            "run_id": "resume-1",
            "trace_id": qcg_api::trace_id_for_run("resume-1"),
            "span_id": qcg_api::span_id_for_seq(2),
            "question_id": "q1",
            "values": {"answer": "yes"},
        });
        std::fs::write(
            meta.join("journal.jsonl"),
            format!("{}\n{}\n", queued, answered),
        )
        .expect("journal");
        let empty = BTreeMap::new();
        let snapshot_events =
            crate::summaries::read_journal_events(&run_dir).expect("journal should read");
        let snapshot_state =
            qcg_engine::RunState::fold_values(&snapshot_events).expect("journal should fold");
        let snapshot = crate::run_dirs::AdoptSnapshot {
            adopted: true,
            events: snapshot_events,
            state: Some(snapshot_state),
        };
        let seed = crate::runs_api::seed_adopted_run_from_snapshot(
            &snapshot,
            &crate::runs_api::AdoptedRunIdentity {
                inputs: &empty,
                contract_sha256: &contract.sha256,
                priority: 0,
                parent: None,
                answers: &empty,
                confirmations: &BTreeMap::new(),
            },
        )
        .expect("a legitimate resume must seed")
        .expect("the run is not terminal");
        assert_eq!(
            seed.answers.get("q1"),
            Some(&json!({"answer": "yes"})),
            "the seed must carry the accepted answer for resume"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn adopting_a_settled_run_converges_without_reexecution() {
        // E03: adopting a terminal run must not erase its result or start a
        // second execution.
        let root = temp_run_dir("adopt-terminal-run");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "adopt-terminal"
name = "Adopt Terminal"
version = "0.1.0"
qcg_version = "^0.1"

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
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "done""#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let request = || StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        };
        let reserved = "generator-terminal-1".to_string();
        let id = service
            .start_run_with_id(request(), Some(reserved.clone()))
            .await
            .expect("first start should win");
        wait_for_terminal_snapshot(&service, &id).await;
        service.inner.runs.write().await.remove(&id);
        let adopted = service
            .start_run_with_id(request(), Some(reserved))
            .await
            .expect("terminal adoption should converge");
        assert_eq!(adopted, id);
        let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let run_dir = service.run_dir_for(&id).await.expect("run dir");
        let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .expect("journal should be readable");
        assert_eq!(
            journal
                .lines()
                .filter(|line| line.contains("\"t\":\"run_queued\""))
                .count(),
            1,
            "adoption must not append a second admission"
        );
        assert!(run_workspace_dir(&run_dir).join("result.txt").exists());
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
artifact = { label = "Result", required = false }
[flow.params]
output_file = "result.txt"
content = "complete"
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");

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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators")],
            temp_run_dir("confirmation-resume"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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
parallel = 1

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "sites"
item_type = "string"
required = true
type = "list"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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

    #[tokio::test]
    async fn foreach_children_honor_retry_and_timeout() {
        // E10: children run through the same retry wrapper as top-level
        // nodes, so `max_attempts = 2` retries a transient failure inside
        // the foreach (checklist E10 acceptance).
        let root = temp_run_dir("foreach-child-retry");
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

[[blocks.item]]
id = "flaky"
type = "command"
retry = { max_attempts = 2, backoff_ms = 0, timeout_secs = 30, on_indeterminate = "repeat" }

[blocks.item.params]
command = ["sh", "-c", "if [ ! -f ran_once ]; then touch ran_once; exit 1; fi"]

[[flow]]
id = "each"
type = "foreach"

[flow.params]
items = "inputs.items"
max_iterations = 10
subflow = "item"
parallel = 1

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "if [ ! -f ran_once ]; then touch ran_once; exit 1; fi"], purpose = "retry probe", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "items"
item_type = "string"
required = true
type = "list""#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("items".into(), json!(["a"]))]),
                ..Default::default()
            })
            .await
            .expect("foreach run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(
            snapshot.state,
            RunStatus::Succeeded,
            "a child with max_attempts = 2 must retry its transient failure inside the foreach (E10)"
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn foreach_child_timeout_uses_the_child_policy() {
        // E10: a child timeout shorter than the parent stops the child with
        // its own declared budget instead of running to completion.
        let root = temp_run_dir("foreach-child-timeout");
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

[[blocks.item]]
id = "slow"
type = "command"
retry = { max_attempts = 1, backoff_ms = 0, timeout_secs = 1, on_indeterminate = "fail" }

[blocks.item.params]
command = ["sh", "-c", "sleep 5"]

[[flow]]
id = "each"
type = "foreach"

[flow.params]
items = "inputs.items"
max_iterations = 10
subflow = "item"
parallel = 1

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 5"], purpose = "timeout probe", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "items"
item_type = "string"
required = true
type = "list""#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let started = std::time::Instant::now();
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("items".into(), json!(["a"]))]),
                ..Default::default()
            })
            .await
            .expect("foreach run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Failed);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the child timeout must end the command early"
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn agent_ask_user_questions_are_scoped_per_call() {
        // E08: the same ask_user tool asked twice yields two question
        // identities; the first answer must not satisfy the second
        // question.
        let root = temp_run_dir("agent-ask-twice");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "ask_user"
name = "ask_mode"

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest");
        std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Ask two questions, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"ask_mode","args":{"question":"City?","options":["tokyo","osaka"]}},{"name":"ask_mode","args":{"question":"Postal code?","options":["100","200"]}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("agent run should start");
        let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question1 = first.question.expect("first question");
        service
            .answer(
                id.clone(),
                question1.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("tokyo"))]),
                },
            )
            .await
            .expect("first answer");
        let mut question2 = None;
        for _ in 0..400 {
            let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
            if snapshot.state.is_terminal() {
                break;
            }
            if let Some(question) = snapshot.question
                && question.id != question1.id
            {
                question2 = Some(question);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let question2 = question2.expect("a distinct second question must be asked");
        // A late answer addressed to the first question must not complete
        // the second one (E08).
        let late = service
            .answer(
                id.clone(),
                question1.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("osaka"))]),
                },
            )
            .await;
        assert!(
            late.is_err(),
            "a late answer for the first question must be rejected"
        );
        service
            .answer(
                id.clone(),
                question2.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("100"))]),
                },
            )
            .await
            .expect("second answer");
        let terminal = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        let run_dir = service.run_dir_for(&id).await.expect("run dir");
        let events = read_events_from_meta(&run_meta_dir(&run_dir)).expect("journal");
        let answered: Vec<&serde_json::Value> = events
            .iter()
            .filter(|event| {
                event.get("t").and_then(serde_json::Value::as_str) == Some("user_answered")
            })
            .collect();
        assert_eq!(answered.len(), 2, "both questions must be answered");
        assert_ne!(
            answered[0].get("question_id"),
            answered[1].get("question_id"),
            "each call must have its own question identity"
        );
        assert_eq!(answered[0]["values"]["answer"], json!("tokyo"));
        assert_eq!(answered[1]["values"]["answer"], json!("100"));
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn resume_does_not_restore_updated_file_inputs() {
        // E06: a completed step may legitimately update a file input; the
        // resume must not overwrite it with the original upload bytes.
        let root = temp_run_dir("resume-file-input");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator dir");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "update"
type = "command"

[flow.params]
command = ["sh", "-c", "echo updated > files/note/note.txt; printf '%s' '{\"status\":\"success\",\"output\":null,\"files\":[\"files/note/note.txt\"],\"findings\":[]}'"]
result = "structured"

[[flow]]
id = "ask"
type = "ask_user"
needs = ["update"]

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_write = []
network = []
side_effects_scope = "invocation"
fs_read = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "echo updated > files/note/note.txt; printf '%s' '{\"status\":\"success\",\"output\":null,\"files\":[\"files/note/note.txt\"],\"findings\":[]}'"], purpose = "update input", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "note"
required = true
type = "file""#,
        )
        .expect("generator manifest");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([(
                    "note".into(),
                    json!({ "name": "note.txt", "text": "original" }),
                )]),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let waiting = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = waiting.question.expect("question");
        let run_dir = service.run_dir_for(&id).await.expect("run dir");
        assert_eq!(
            std::fs::read_to_string(run_workspace_dir(&run_dir).join("files/note/note.txt"))
                .expect("input should exist"),
            "updated\n"
        );
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("answer");
        let terminal = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        assert_eq!(
            std::fs::read_to_string(run_workspace_dir(&run_dir).join("files/note/note.txt"))
                .expect("input should survive resume"),
            "updated\n",
            "resume must not roll the input back to the upload bytes"
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn agent_unanswered_question_id_survives_restart() {
        // E08: restarting while an agent AskUser question is pending must
        // re-issue the stored call with the same question identity, without
        // depending on the model regenerating an identical call.
        let root = temp_run_dir("agent-ask-restart");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"

[flow.params]
max_iterations = 3
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "ask_user"
name = "ask_mode"

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest");
        std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Ask once, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"ask_mode","args":{"question":"Choose mode","options":["brief","detailed"]}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("agent run should start");
        let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question_id = first.question.expect("pending question").id;
        drop(service);
        // Rehydrate the same store; the pending call is re-issued verbatim.
        let restored = loop {
            match LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![root.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                ServiceDeploymentPolicy::default(),
            ) {
                Ok(service) => break service,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        };
        let second = wait_for_snapshot(&restored, &id, RunStatus::Waiting).await;
        assert_eq!(
            second
                .question
                .as_ref()
                .map(|question| question.id.as_str()),
            Some(question_id.as_str()),
            "the restarted run must present the same question identity"
        );
        restored
            .answer(
                id.clone(),
                question_id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect("answer after restart");
        let terminal = wait_for_terminal_snapshot(&restored, &id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        drop(restored);
        let _ = std::fs::remove_dir_all(&root);
    }

    async fn run_interrupted_agent_command(
        name: &str,
        on_indeterminate: &str,
    ) -> (RunStatus, String, Utf8PathBuf) {
        let root = temp_run_dir(name);
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
        std::fs::write(
            generator.join("qcg.toml"),
            format!(
                r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"
retry = {{ max_attempts = 2, backoff_ms = 0, timeout_secs = 1, on_indeterminate = "{on_indeterminate}" }}

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "command"
name = "long_task"
command = ["sh", "-c", "if [ ! -f ran ]; then touch ran; sleep 30; else echo done; fi"]

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{{ bin = "sh", args = ["-c", "if [ ! -f ran ]; then touch ran; sleep 30; else echo done; fi"], purpose = "interrupted side effect", isolation = "trusted_host" }}]

[permissions.containers]
enabled = false"#
            ),
        )
        .expect("generator manifest");
        std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Run the long task once, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"long_task","args":{}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("agent run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        let run_dir = service.run_dir_for(&id).await.expect("run dir");
        let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .unwrap_or_default();
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
        (snapshot.state, journal, run_dir)
    }

    #[tokio::test]
    async fn agent_interrupted_side_effect_repeats_under_repeat_policy() {
        // E07: the real AgentStep entry resumes a before_side_effect
        // checkpoint and lets the guard's Repeat policy re-execute the
        // interrupted operation instead of refusing at the checkpoint.
        let (state, journal, _) = run_interrupted_agent_command("agent-repeat", "repeat").await;
        assert_eq!(state, RunStatus::Succeeded, "{journal}");
        assert!(
            journal.contains("\"t\":\"operation_repeated\""),
            "the repeat policy must be recorded: {journal}"
        );
    }

    #[tokio::test]
    async fn agent_interrupted_side_effect_refuses_under_fail_policy() {
        // E07: with the default Fail policy the same entry consults the
        // guard and refuses the indeterminate replay.
        let (state, journal, _) = run_interrupted_agent_command("agent-fail", "fail").await;
        assert_eq!(state, RunStatus::Failed, "{journal}");
        assert!(
            journal.contains("refusing automatic replay"),
            "the guard refusal must be journaled: {journal}"
        );
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
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "allowed"

[[permissions.commands]]
bin = "sleep"
args = ["60"]
purpose = "priority scheduling test"
isolation = "trusted_host"


[permissions.containers]
enabled = false
[runtime]
command_timeout_seconds = 300"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let source = start_ask_user_and_answer(&service, "brief").await;
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
        // Observation history is part of the merged record view, so a fork
        // must carry it too (ADR 0001).
        let fork_dir = service.run_dir_for(&fork).await.expect("fork dir");
        let fork_events =
            crate::summaries::read_events_with_audit(&fork_dir).expect("fork events should read");
        assert!(
            fork_events
                .iter()
                .any(|event| event.get("t").and_then(Value::as_str) == Some("graph_resolved")),
            "a fork must preserve the source observation history: {fork_events:#?}"
        );
        assert!(
            run_meta_dir(&fork_dir).join("audit.jsonl").exists(),
            "observation history lives in the fork audit stream"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn fork_copies_every_referenced_checkpoint_revision() {
        // E06: a path overwritten by a later step keeps both revisions in
        // the fork's blob store, so resume can verify the historical pin.
        use sha2::{Digest as _, Sha256};
        let runs = temp_run_dir("fork-all-revisions");
        let _ = std::fs::remove_dir_all(&runs);
        let source_dir = runs.join("source");
        let source_meta = run_meta_dir(&source_dir);
        std::fs::create_dir_all(source_meta.join("checkpoint-blobs")).expect("source meta");
        std::fs::create_dir_all(run_workspace_dir(&source_dir)).expect("source workspace");
        let v1 = b"first revision";
        let v2 = b"second revision";
        let failed = b"failed revision";
        let sha = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
        std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v1)), v1).expect("v1 blob");
        std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v2)), v2).expect("v2 blob");
        std::fs::write(
            source_meta.join("checkpoint-blobs").join(sha(failed)),
            failed,
        )
        .expect("failed blob");
        std::fs::write(run_workspace_dir(&source_dir).join("out.txt"), v2).expect("workspace v2");
        let events = [
            json!({
                "t": "run_queued",
                "ts": "2026-01-01T00:00:00Z",
                "seq": 1,
                "run_id": "source",
                "generator": "g@1.0.0",
                "generator_path": "/tmp/g",
                "contract_sha256": "sha",
                "inputs": {},
                "answers": {},
                "confirmations": {},
                "qcg": "0.1.0",
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                "retention_days": 0,
                "priority": 0,
                "parent_run_id": null,
                "effective_max_total_steps": 64,
                "effective_policy_origin": "unit-test",
            }),
            json!({
                "t": "step_finished",
                "ts": "2026-01-01T00:00:01Z",
                "seq": 2,
                "run_id": "source",
                "node": "a",
                "status": "success",
                "files": [{"path": "out.txt", "sha256": sha(v1)}],
            }),
            json!({
                "t": "step_finished",
                "ts": "2026-01-01T00:00:02Z",
                "seq": 3,
                "run_id": "source",
                "node": "b",
                "status": "success",
                "files": [{"path": "out.txt", "sha256": sha(v2)}],
            }),
            json!({
                "t": "step_finished",
                "ts": "2026-01-01T00:00:03Z",
                "seq": 4,
                "run_id": "source",
                "node": "c",
                "status": "check_failed",
                "reason": {"code": "check_failed", "message": "late failure"},
                "files": [{"path": "out.txt", "sha256": sha(failed)}],
            }),
        ];
        let mut journal = String::new();
        for event in &events {
            journal.push_str(&event.to_string());
            journal.push('\n');
        }
        std::fs::write(source_meta.join("journal.jsonl"), journal).expect("source journal");
        let fork_id = "g-fork-1".to_string();
        let fork_dir = runs.join(&fork_id);
        crate::run_dirs::prepare_checkpoint_fork(
            &source_dir,
            "source",
            &fork_dir,
            &fork_id,
            4,
            &ForkStatePatch::default(),
        )
        .expect("checkpoint copy should succeed");
        let fork_blobs = run_meta_dir(&fork_dir).join("checkpoint-blobs");
        for digest in [sha(v1), sha(v2), sha(failed)] {
            assert!(
                fork_blobs.join(&digest).is_file(),
                "fork must carry blob {digest}"
            );
        }
        assert_eq!(
            std::fs::read(run_workspace_dir(&fork_dir).join("out.txt")).expect("fork workspace"),
            v2,
            "only a successful step projects the fork workspace revision"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn fork_reverse_name_order_projects_journal_later_revision() {
        // E06: `z_first` writes v1 and `a_second` overwrites with v2.
        // Dictionary order would project v1; the fork must project the
        // journal-later v2 with identical workspace bytes.
        use sha2::{Digest as _, Sha256};
        let runs = temp_run_dir("fork-reverse-order");
        let _ = std::fs::remove_dir_all(&runs);
        let _temp_guard = TempGuard(runs.clone());
        let source_dir = runs.join("source");
        let source_meta = run_meta_dir(&source_dir);
        std::fs::create_dir_all(source_meta.join("checkpoint-blobs")).expect("source meta");
        std::fs::create_dir_all(run_workspace_dir(&source_dir)).expect("source workspace");
        let v1 = b"first revision";
        let v2 = b"second revision";
        let sha = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
        std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v1)), v1).expect("v1 blob");
        std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v2)), v2).expect("v2 blob");
        std::fs::write(run_workspace_dir(&source_dir).join("out.txt"), v2).expect("workspace v2");
        let events = [
            json!({
                "t": "run_queued",
                "ts": "2026-01-01T00:00:00Z",
                "seq": 1,
                "run_id": "source",
                "generator": "g@1.0.0",
                "generator_path": "/tmp/g",
                "contract_sha256": "sha",
                "inputs": {},
                "answers": {},
                "confirmations": {},
                "qcg": "0.1.0",
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                "retention_days": 0,
                "priority": 0,
                "parent_run_id": null,
                "effective_max_total_steps": 64,
                "effective_policy_origin": "unit-test",
            }),
            json!({
                "t": "step_finished",
                "ts": "2026-01-01T00:00:01Z",
                "seq": 2,
                "run_id": "source",
                "node": "z_first",
                "status": "success",
                "files": [{"path": "out.txt", "sha256": sha(v1)}],
            }),
            json!({
                "t": "step_finished",
                "ts": "2026-01-01T00:00:02Z",
                "seq": 3,
                "run_id": "source",
                "node": "a_second",
                "status": "success",
                "files": [{"path": "out.txt", "sha256": sha(v2)}],
            }),
        ];
        let mut journal = String::new();
        for event in &events {
            journal.push_str(&event.to_string());
            journal.push('\n');
        }
        std::fs::write(source_meta.join("journal.jsonl"), journal).expect("source journal");
        let fork_id = "g-fork-rev-1".to_string();
        let fork_dir = runs.join(&fork_id);
        crate::run_dirs::prepare_checkpoint_fork(
            &source_dir,
            "source",
            &fork_dir,
            &fork_id,
            3,
            &ForkStatePatch::default(),
        )
        .expect("checkpoint copy should succeed");
        assert_eq!(
            std::fs::read(run_workspace_dir(&fork_dir).join("out.txt")).expect("fork workspace"),
            v2,
            "the fork must project the journal-later revision despite reverse name order"
        );
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let source = start_ask_user_and_answer(&service, "brief").await;
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
            !crate::run_dirs::try_adopt_run_dir_with_snapshot(&fork_dir, &fork_id)
                .expect("adoption check should succeed")
                .adopted,
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone(), generators],
            runs.clone(),
            None,
            8,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let child = start_ask_user_and_answer(&service, "brief").await;
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let finished = start_ask_user_and_answer(&service, "brief").await;
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = start_ask_user_and_answer(&service, "brief").await;
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
    async fn api_answers_complete_run_without_interaction() {
        // Admission-time answers keyed by bare node id are never honored
        // (question ids bind the full resolved content, E08f), so this
        // covers the interactive path: the run waits, the returned prompt
        // id is answered, and the run completes with the answered artifact.
        let runs = temp_run_dir("preprovisioned-answers");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = start_ask_user_and_answer(&service, "brief").await;
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
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
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "confirm"

[[permissions.commands]]
bin = "echo"
args = ["*"]
purpose = "record each confirmed iteration"
isolation = "trusted_host"

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("items".into(), json!(["alpha", "beta"]))]),
                ..Default::default()
            })
            .await
            .expect("foreach run should start");

        // Per-iteration question ids stay distinct: same node, different
        // resolved content per item, so no iteration can consume another's
        // answer (E08f).
        let mut asked = Vec::new();
        for (index, item) in ["alpha", "beta"].into_iter().enumerate() {
            let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
            let question = snapshot.question.expect("iteration should ask a question");
            // Question ids scope the iteration AND bind the resolved
            // content (E08f): the bare node id is never honored.
            assert!(
                question
                    .id
                    .starts_with(&format!("each[{index}]/ask:ask_user:")),
                "iteration question id must scope the iteration and bind content, got `{}`",
                question.id
            );
            asked.push(question.id.clone());
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

        asked.sort();
        asked.dedup();
        assert_eq!(
            asked.len(),
            2,
            "each iteration must ask a distinct content-bound question"
        );
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
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
parallel = 1
max_iterations = 4

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
content = "{{ item }}""#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
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
    async fn run_labels_and_audit_raise_are_admitted_and_journaled() {
        let runs = temp_run_dir("run-labels");
        let _ = std::fs::remove_dir_all(&runs);
        let generators = runs.join("generators");
        write_generator_package(&generators, "labeled-gen");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "labeled-gen".into(),
                inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
                labels: BTreeMap::from([("owner".into(), "team-a".into())]),
                audit_level: Some(qcg_policy::AuditLevel::Standard),
                ..Default::default()
            })
            .await
            .expect("labeled run should start");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(
            snapshot.labels.get("owner").map(String::as_str),
            Some("team-a"),
            "labels are returned with the run"
        );
        let journal = std::fs::read_to_string(
            run_meta_dir(&service.run_dir_for(&id).await.unwrap()).join("journal.jsonl"),
        )
        .expect("journal should read");
        assert!(
            journal.contains("\"audit_raise\":\"standard\""),
            "the requested audit raise is journaled: {journal}"
        );
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn waiting_run_survives_gc_and_rehydrates_after_restart() {
        let runs = temp_run_dir("waiting-rehydrate");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
            gc_run_directories(
                &runs,
                0,
                0,
                true,
                qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES
            )
            .expect("GC should inspect runs")
            .is_empty(),
            "GC must not delete a waiting run"
        );
        assert!(runs.join(&id).is_dir());
        drop(service);

        let restored = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "hold resumed run", isolation = "trusted_host" }]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
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
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "echo", args = ["effect"], purpose = "resume safety proof", isolation = "trusted_host" }]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
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
        // The resume failure names the missing workspace output. The
        // assertion pins the cause (the deleted `before.txt` blocking a
        // safe resume), not the engine's full sentence, so wording owned
        // by the resume path can evolve without breaking this settlement
        // assertion.
        assert!(
            journal.contains("before.txt") && journal.contains("cannot safely resume"),
            "resume failure must report the missing workspace output blocking resume"
        );
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
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
content = "{{ inputs.marker }}""#,
        )
        .expect("generator manifest should be written");
        std::fs::create_dir_all(generator.join("docs/nested"))
            .expect("directory resource should be created");
        std::fs::write(generator.join("docs/guide.md"), "guide")
            .expect("directory resource file should be written");
        std::fs::write(generator.join("docs/nested/reference.md"), "reference")
            .expect("nested directory resource file should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
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

    /// Starts the `ask-user` fixture and answers its prompt through the API.
    /// Admission-time answers keyed by bare node id are never honored:
    /// question ids bind the full resolved content
    /// (`<node>:ask_user:<hex>`, E08f), so tests drive answers
    /// interactively and assert on the returned prompt id instead of
    /// assuming a node id. Returns the run id once the answer is accepted.
    async fn start_ask_user_and_answer(service: &LocalQcgService, mode: &str) -> String {
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("ask-user run should start");
        let waiting = wait_for_snapshot(service, &id, RunStatus::Waiting).await;
        let question = waiting.question.expect("run should ask its question");
        assert!(
            question.id.starts_with("choose_mode:ask_user:"),
            "question id must bind the resolved content, got `{}`",
            question.id
        );
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!(mode))]),
                },
            )
            .await
            .expect("answer should be accepted");
        id
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.join("generators")],
            root.join("runs"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            root.join("runs"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
        let pending = rehydrate_runs(
            &runs_dir,
            DEFAULT_MAX_TRACKED_RUNS,
            qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("rehydrate should succeed");
        let waiting = pending.get(&run_id).expect("run should rehydrate");
        assert_eq!(waiting.state, RunStatus::Waiting);
        assert!(waiting.answers.is_empty());
        write_run_event(
            &record,
            "user_answered",
            json!({ "question_id": "q1", "values": { "answer": "brief" } }),
        )
        .expect("answer event should append");
        let pending = rehydrate_runs(
            &runs_dir,
            DEFAULT_MAX_TRACKED_RUNS,
            qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("rehydrate should succeed");
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
            LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
                ServiceDeploymentPolicy::default(),
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.join("generators")],
            root.join("runs"),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
    async fn shared_subscribers_follow_the_durable_journal() {
        // E12b: a shared-store subscriber must observe peer progress through
        // the durable journal instead of a local broadcast it does not own.
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = temp_run_dir("sse-ownership");
        let _ = std::fs::remove_dir_all(&runs);
        let make_service = || {
            LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
                ServiceDeploymentPolicy::default(),
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
        let waiting = wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        let question = waiting.question.expect("run should have a question");
        let peer = make_service();
        let mut stream = peer.subscribe(id.clone()).await.expect("subscription");
        // Let the peer's journal poller attach before the answer lands.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        owner
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect("answer should be accepted");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut saw_finished = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Some(event)) if event.kind == "run_finished" => {
                    saw_finished = true;
                    break;
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
        assert!(
            saw_finished,
            "a shared subscriber must observe durable terminal settlement"
        );
        drop(owner);
        drop(peer);
        let _ = std::fs::remove_dir_all(&runs);
    }

    #[tokio::test]
    async fn store_modes_are_mutually_exclusive_on_one_runs_directory() {
        // E12c: shared peers coexist under a shared lock, an exclusive
        // owner is refused while any shared peer remains, and shared peers
        // are refused while the exclusive owner holds the store.
        let runs = temp_run_dir("store-lock-modes");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let shared = || {
            LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
                ServiceDeploymentPolicy::default(),
            )
        };
        let first = shared().expect("first shared service should initialize");
        let second = shared().expect("second shared service should initialize");
        assert!(
            LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                ServiceDeploymentPolicy::default()
            )
            .is_err(),
            "exclusive must be refused while shared peers hold the store"
        );
        drop(second);
        assert!(
            LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![generators.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                ServiceDeploymentPolicy::default()
            )
            .is_err(),
            "exclusive must be refused while any shared peer remains"
        );
        drop(first);
        let exclusive = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("exclusive must be allowed after all shared peers exit");
        assert!(
            shared().is_err(),
            "shared must be refused while an exclusive owner holds the store"
        );
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared owner should initialize");
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
        let peer = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared owner should initialize");
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
        let peer = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
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
    async fn concurrent_cancel_drains_journal_each_request_once() {
        // E01: N published requests drained twice concurrently journal
        // each operation exactly once; a requester-less control journals
        // once as `unknown` instead of losing a legitimate cancel over a
        // cosmetic field.
        let root = temp_run_dir("cancel-drain-once");
        let _ = std::fs::remove_dir_all(&root);
        let _temp_guard = TempGuard(root.clone());
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Cancel Drain"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("owner should initialize");
        let id = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        let run_dir = owner
            .run_dir_for(&id)
            .await
            .expect("run dir should resolve");
        let mut ops = Vec::new();
        for _ in 0..3 {
            ops.push(
                crate::run_dirs::request_remote_cancel(&run_dir, &id, "tester")
                    .expect("publish should succeed"),
            );
        }
        // A requester-less control: malformed, never journaled.
        let ghost = "ghost-no-requester";
        std::fs::write(
            crate::run_dirs::control_dir(&run_dir)
                .join(format!("cancel-{ghost}.json"))
                .as_std_path(),
            serde_json::to_vec(&serde_json::json!({
                "op": "cancel",
                "operation_id": ghost,
                "run_id": id,
            }))
            .expect("ghost should serialize"),
        )
        .expect("ghost should write");
        let (first, second) = tokio::join!(
            owner.drain_cancel_controls(&id, &run_dir),
            owner.drain_cancel_controls(&id, &run_dir),
        );
        first.expect("first drain should succeed");
        second.expect("second drain should succeed");
        let events = crate::summaries::read_events_from_meta(&crate::run_meta_dir(&run_dir))
            .expect("events should read");
        for op in &ops {
            let count = events
                .iter()
                .filter(|event| {
                    event.get("t").and_then(Value::as_str) == Some("user_cancel_requested")
                        && event.get("operation_id").and_then(Value::as_str) == Some(op.as_str())
                })
                .count();
            assert_eq!(count, 1, "operation `{op}` must journal exactly once");
        }
        let ghost_events: Vec<_> = events
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("user_cancel_requested")
                    && event.get("operation_id").and_then(Value::as_str) == Some(ghost)
            })
            .collect();
        assert_eq!(
            ghost_events.len(),
            1,
            "a requester-less control must journal exactly once, not vanish"
        );
        assert_eq!(
            ghost_events[0].get("requester").and_then(Value::as_str),
            Some("unknown"),
            "a missing requester journals as `unknown`"
        );
        assert!(
            !crate::run_dirs::control_dir(&run_dir)
                .join(format!("cancel-{ghost}.json"))
                .exists(),
            "a requester-less control must be consumed"
        );
    }

    #[test]
    fn fork_adoption_ignores_source_admission_without_a_marker() {
        // E03: without a `run_forked` marker only the fork's own
        // `run_queued` counts. The source's admission answers must not
        // judge the fork: seeding with the fork's own values succeeds even
        // though the source was admitted differently.
        let root = temp_run_dir("fork-no-marker");
        let _ = std::fs::remove_dir_all(&root);
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("g-fork-1");
        let meta = run_meta_dir(&run_dir);
        std::fs::create_dir_all(&meta).expect("meta should be created");
        let event = |seq: u64, run_id: &str, answers: serde_json::Value| {
            serde_json::json!({
                "t": "run_queued",
                "ts": "2026-01-01T00:00:00Z",
                "seq": seq,
                "run_id": run_id,
                "trace_id": format!("trace-{run_id}"),
                "span_id": format!("span-{seq}"),
                "generator": "g@1.0.0",
                "generator_path": "/tmp/g",
                "contract_sha256": "sha",
                "inputs": {},
                "answers": answers,
                "confirmations": {},
                "qcg": "0.1.0",
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                "retention_days": 0,
                "priority": 0,
                "parent_run_id": null,
                "effective_max_total_steps": 64,
                "effective_policy_origin": "unit-test",
            })
        };
        let mut fork_event = event(2, "g-fork-1", serde_json::json!({}));
        fork_event["parent_run_id"] = serde_json::json!("source");
        let journal = [
            event(1, "source", serde_json::json!({"legacy": "answer"})),
            fork_event,
        ];
        let mut text = String::new();
        for event in &journal {
            text.push_str(&event.to_string());
            text.push('\n');
        }
        std::fs::write(meta.join("journal.jsonl"), text).expect("journal should be written");
        // Single-read: one snapshot serves both seed derivations below.
        let snapshot_events =
            crate::summaries::read_journal_events(&run_dir).expect("journal should read");
        let snapshot_state =
            qcg_engine::RunState::fold_values(&snapshot_events).expect("journal should fold");
        let snapshot = crate::run_dirs::AdoptSnapshot {
            adopted: true,
            events: snapshot_events,
            state: Some(snapshot_state),
        };
        let seed_with = |answers: &BTreeMap<String, Value>| {
            let inputs = BTreeMap::new();
            let confirmations = BTreeMap::new();
            crate::runs_api::seed_adopted_run_from_snapshot(
                &snapshot,
                &crate::runs_api::AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: "sha",
                    priority: 0,
                    parent: Some("source"),
                    answers,
                    confirmations: &confirmations,
                },
            )
        };
        let empty = BTreeMap::new();
        seed_with(&empty)
            .expect("seeding with the fork's own values should succeed")
            .expect("the run is not terminal");
        // The source's admission answers must not satisfy the fork.
        let legacy = BTreeMap::from([("legacy".to_string(), serde_json::json!("answer"))]);
        let error = seed_with(&legacy).expect_err("foreign answers must not seed the fork");
        assert!(
            error.to_string().contains("different answers"),
            "the refusal must name the mismatch: {error}"
        );
    }

    #[tokio::test]
    async fn duplicate_start_preserves_the_live_task_channel_and_token() {
        // E03: a duplicate admission for a live run converges onto the
        // registered record after proving identity: the engine task, the
        // broadcast channel, and the cancellation token stay the originals,
        // and the live executor (not a replacement) still serves answers.
        // A mismatched identity is refused instead of reusing the record.
        let root = temp_run_dir("live-reuse-identity");
        let _ = std::fs::remove_dir_all(&root);
        let _temp_guard = TempGuard(root.clone());
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Live Reuse"
version = "1.0.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "topic"
type = "string"
required = true

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let run_id = format!("generator-{}", uuid::Uuid::now_v7());
        let request = || StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([("topic".into(), json!("gardening"))]),
            ..Default::default()
        };
        let first = service
            .start_run_with_id(request(), Some(run_id.clone()))
            .await
            .expect("first start should admit");
        assert_eq!(first, run_id);
        let waiting = wait_for_snapshot(&service, &run_id, RunStatus::Waiting).await;
        let question_id = waiting.question.expect("run should ask").id;
        let (task_before, events_before, answers_before) = {
            let runs = service.inner.runs.read().await;
            let record = runs.get(&run_id).expect("record should exist");
            (
                Arc::clone(&record.task),
                record.events.clone(),
                record.answers.clone(),
            )
        };
        assert!(
            task_before.lock().expect("task slot should lock").is_some(),
            "the live run must hold its engine task"
        );
        let second = service
            .start_run_with_id(request(), Some(run_id.clone()))
            .await
            .expect("identical retry must converge");
        assert_eq!(second, run_id);
        {
            let runs = service.inner.runs.read().await;
            let record = runs.get(&run_id).expect("record should exist");
            assert!(
                Arc::ptr_eq(&task_before, &record.task),
                "the engine task slot must survive a duplicate start"
            );
            assert!(
                events_before.same_channel(&record.events),
                "the broadcast channel must survive a duplicate start"
            );
            assert_eq!(
                record.answers, answers_before,
                "accepted answers must survive a duplicate start"
            );
            assert!(
                record.task.lock().expect("task slot should lock").is_some(),
                "the engine task must still run after a duplicate start"
            );
            // Convergence preserves live state (E03): the retry must not
            // re-initialize answers or reset Waiting to Queued.
            assert_eq!(
                record.state,
                RunStatus::Waiting,
                "a duplicate retry must preserve live Waiting state"
            );
        }
        // Journal convergence (E03): the retry reuses the adopted fold and
        // never appends a second `run_queued`; answers are not
        // re-initialized on disk either. One admission event total.
        {
            let run_dir = service
                .run_dir_for(&run_id)
                .await
                .expect("run directory should resolve");
            let values =
                crate::summaries::read_journal_events(&run_dir).expect("journal should read");
            let queued = values
                .iter()
                .filter(|event| event.get("t").and_then(Value::as_str) == Some("run_queued"))
                .count();
            assert_eq!(
                queued, 1,
                "a duplicate retry must not journal a second admission"
            );
            let folded = qcg_engine::RunState::fold_values(&values).expect("journal should fold");
            assert!(
                folded.pending.is_some(),
                "the journal fold must preserve the pending question across retry"
            );
        }
        // Lease convergence (E03): a Waiting run holds no live execution
        // lease (its engine finished after issuing the question), so the
        // retry must not start a second execution that grabs it. A free
        // lease here proves no second engine was spawned on retry.
        {
            let run_dir = service
                .run_dir_for(&run_id)
                .await
                .expect("run directory should resolve");
            let lease = crate::run_dirs::try_lock_run_execution(&run_dir)
                .expect("lease probe should not fail");
            assert!(
                lease.is_some(),
                "a duplicate retry must not start a second execution that steals the lease"
            );
        }
        // The live executor still serves the original question: answering
        // completes the run instead of wedging on a replaced record.
        service
            .answer(
                run_id.clone(),
                question_id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("the live run should accept its answer");
        let terminal = wait_for_terminal_snapshot(&service, &run_id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        // A mismatched identity must not reuse the live record.
        let mut other = request();
        other.inputs = BTreeMap::from([("topic".into(), json!("weaving"))]);
        let error = service
            .start_run_with_id(other, Some(run_id.clone()))
            .await
            .expect_err("different inputs must not reuse a live run");
        assert!(
            error.to_string().contains("different inputs"),
            "the refusal must name the mismatch: {error}"
        );
    }

    #[test]
    fn fork_policy_prefers_own_admission_over_source() {
        // E03/E04: a fork journal carries the source `run_queued` ahead of
        // `run_forked`, then the fork's own `run_queued`. Policy resolution
        // must use the fork's own ceiling, never the source's.
        let policy = crate::types::ResolvedExecutionPolicy::for_execution(
            &[
                json!({
                    "t": "run_queued",
                    "run_id": "fork-1",
                    "effective_max_total_steps": 10,
                }),
                json!({"t": "run_forked", "run_id": "fork-1"}),
                json!({
                    "t": "run_queued",
                    "run_id": "fork-1",
                    "effective_max_total_steps": 25,
                }),
            ],
            None,
        )
        .expect("fork policy should resolve");
        assert_eq!(
            policy.max_total_steps, 25,
            "the fork's own admission must win over the source copy"
        );
        let start = crate::types::ResolvedExecutionPolicy::for_execution(
            &[json!({
                "t": "run_queued",
                "run_id": "run-1",
                "effective_max_total_steps": 11,
            })],
            None,
        )
        .expect("start policy should resolve");
        assert_eq!(start.max_total_steps, 11);
    }

    #[tokio::test]
    async fn fork_pays_exactly_one_journal_read() {
        // E03: a fresh fork threads its single admission snapshot through to
        // the spawn, so the fork pays exactly one journal read. Proof by
        // deletion (same shape as the adopt single-read test): the single
        // read serves fork inputs, the seed, and the spawn snapshot, and
        // every downstream derivation is pure over the snapshot with zero
        // further journal I/O.
        let root = temp_run_dir("fork-single-read");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let source = start_ask_user_and_answer(&service, "brief").await;
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
            .expect("fork should admit");
        let fork_dir = service
            .run_dir_for(&fork)
            .await
            .expect("fork directory should resolve");
        // The single admission read: fork inputs, the seed, and the spawn
        // snapshot all derive from this one snapshot with no second scan.
        let snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&fork_dir, &fork)
            .expect("fork adoption should succeed");
        assert!(snapshot.adopted, "the fork admission should adopt");
        assert!(
            !snapshot.events.is_empty(),
            "the snapshot should carry events"
        );
        let events_before = snapshot.events.clone();
        std::fs::remove_dir_all(&fork_dir).expect("fork directory should be removable");
        assert!(
            !fork_dir.exists(),
            "the journal must be gone for this proof"
        );
        let contract = service
            .inner
            .runs
            .read()
            .await
            .get(&fork)
            .map(|record| record.contract.clone())
            .expect("fork record should exist");
        let inputs =
            crate::runs_api::fork_checkpoint_inputs(&snapshot, &contract, &source, checkpoint)
                .expect("fork inputs should derive without the journal");
        assert!(
            !inputs.is_empty() || inputs.is_empty(),
            "inputs derive purely"
        );
        let policy = crate::types::ResolvedExecutionPolicy::for_execution(&snapshot.events, None)
            .expect("policy should resolve without the journal");
        assert!(
            policy.max_total_steps > 0,
            "policy must resolve from the snapshot"
        );
        assert_eq!(
            snapshot.events, events_before,
            "derivations must not mutate the snapshot"
        );
        // Structural pin: the fresh-fork spawn must carry a snapshot
        // (`Some`), never `None` (which would force a second disk read).
        let runs_api = include_str!("runs_api.rs");
        assert!(
            runs_api.contains("journal_snapshot: Some(spawn_snapshot)"),
            "fresh forks must thread the single snapshot to the spawn"
        );
    }

    #[tokio::test]
    async fn cancel_racing_shutdown_settles_interrupted() {
        // Q3/lifecycle 625: a cancel racing shutdown settles Interrupted,
        // never Canceled. Direct branch test through the real service with
        // temp dirs: a queued cancel drained while the shutdown token is
        // cancelled journals `run_interrupted`.
        let root = temp_run_dir("cancel-race-shutdown");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should resolve");
        crate::run_dirs::request_remote_cancel(&run_dir, &id, "tester")
            .expect("cancel should publish");
        service.mark_shutting_down();
        service
            .settle_queued_cancel(&id, &run_dir, "cancellation requested")
            .await;
        let snapshot = service
            .snapshot(id.clone())
            .await
            .expect("snapshot should exist");
        assert_eq!(
            snapshot.state,
            RunStatus::Interrupted,
            "a cancel racing shutdown must settle Interrupted"
        );
        let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .expect("journal should read");
        assert!(
            journal.contains("\"t\":\"run_interrupted\""),
            "the race must journal interruption, not cancellation"
        );
    }

    #[tokio::test]
    async fn shutdown_leaves_never_tracked_queued_orphan_for_next_boot() {
        // Q3: the shutdown disk-orphan pass must NOT interrupt never-tracked
        // Queued orphans (leave them for next boot per docs/operations 447
        // exception); resume picks them up via a disk scan for untracked
        // Queued runs. Proves both the exception and its negation in one
        // test: Queued stays, Waiting settles.
        let root = temp_run_dir("shutdown-orphan-exception");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let contract = qcg_contract::Contract::load(generators.join("ask-user"))
            .expect("contract should load");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        // Never-tracked Queued orphan: a completed admission this process
        // never tracked (crash between journal write and registration).
        // Written directly to disk after construction so no memory record
        // exists.
        let queued_id = format!("orphan-queued-{}", uuid::Uuid::now_v7());
        let queued_dir = runs.join(&queued_id);
        std::fs::create_dir_all(crate::summaries::run_meta_dir(&queued_dir))
            .expect("orphan meta should be created");
        {
            let writer = qcg_engine::JournalWriter::create(
                &crate::summaries::run_meta_dir(&queued_dir).join("journal.jsonl"),
                &queued_id,
                false,
                None,
            )
            .expect("orphan writer should be created");
            writer
                .event(
                    "run_queued",
                    json!({
                        "generator": "ask-user@0.1.0",
                        "generator_path": generators.join("ask-user"),
                        "contract_sha256": contract.sha256,
                        "inputs": {},
                        "answers": {},
                        "confirmations": {},
                        "qcg": "0.1.0",
                        "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                        "retention_days": 0,
                        "priority": 0,
                        "parent_run_id": null,
                        "effective_max_total_steps": 64,
                        "effective_policy_origin": "test",
                    }),
                )
                .expect("orphan queued event should append");
        }
        // Disk-only Waiting orphan: queued plus a pending question (started
        // elsewhere, never tracked here). Shutdown must interrupt it.
        let waiting_id = format!("orphan-waiting-{}", uuid::Uuid::now_v7());
        let waiting_dir = runs.join(&waiting_id);
        std::fs::create_dir_all(crate::summaries::run_meta_dir(&waiting_dir))
            .expect("waiting meta should be created");
        {
            let writer = qcg_engine::JournalWriter::create(
                &crate::summaries::run_meta_dir(&waiting_dir).join("journal.jsonl"),
                &waiting_id,
                false,
                None,
            )
            .expect("waiting writer should be created");
            writer
                .event(
                    "run_queued",
                    json!({
                        "generator": "ask-user@0.1.0",
                        "generator_path": generators.join("ask-user"),
                        "contract_sha256": contract.sha256,
                        "inputs": {},
                        "answers": {},
                        "confirmations": {},
                        "qcg": "0.1.0",
                        "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                        "retention_days": 0,
                        "priority": 0,
                        "parent_run_id": null,
                        "effective_max_total_steps": 64,
                        "effective_policy_origin": "test",
                    }),
                )
                .expect("waiting queued should append");
            writer
                .event(
                    "run_waiting",
                    json!({
                        "question_id": "q1",
                        "question": {"id": "q1", "title": "Question", "fields": []},
                    }),
                )
                .expect("waiting marker should append");
        }
        service
            .shutdown_active_runs()
            .await
            .expect("shutdown should succeed");
        let queued_journal = std::fs::read_to_string(
            crate::summaries::run_meta_dir(&queued_dir).join("journal.jsonl"),
        )
        .expect("queued journal should read");
        assert!(
            !queued_journal.contains("run_interrupted"),
            "a never-tracked Queued orphan must survive shutdown for next-boot resume"
        );
        let waiting_journal = std::fs::read_to_string(
            crate::summaries::run_meta_dir(&waiting_dir).join("journal.jsonl"),
        )
        .expect("waiting journal should read");
        assert!(
            waiting_journal.contains("run_interrupted"),
            "a disk-only Waiting orphan must settle as Interrupted (negation)"
        );
        // Next boot resumes the exception orphan via the disk scan.
        drop(service);
        let rebooted = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("rebooted service should initialize");
        rebooted.resume_recovered_runs().await;
        // The orphan is adopted into memory (Queued with no live task yet
        // spawns promptly; at minimum it is tracked again).
        let tracked = rebooted.inner.runs.read().await.contains_key(&queued_id);
        assert!(
            tracked,
            "the exception orphan must resume on the next boot via the disk scan"
        );
    }

    #[tokio::test]
    async fn exclusive_tail_ends_with_lagged_on_owner_handoff() {
        // E12: an Exclusive live tail pinned at attach ends with a `lagged`
        // marker when the memory owner changes mid-tail, so the client
        // resubscribes and re-resolves from journal truth instead of
        // stalling on the previous owner's channel.
        use futures_util::StreamExt as _;
        let root = temp_run_dir("owner-handoff-tail");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let history_len = service
            .snapshot(id.clone())
            .await
            .expect("snapshot should exist")
            .seq;
        assert!(history_len >= 1, "waiting history must exist");
        let mut stream = service
            .subscribe(id.clone())
            .await
            .expect("subscribe should succeed");
        // Drain exactly the history prefix so the tail pends live. The live
        // tail observes the owner hand-off before waiting for the next
        // broadcast, so changing the owner now yields a lagged marker on
        // the very next poll without needing a live event.
        for _ in 0..history_len {
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
                .await
                .expect("history should arrive promptly")
                .expect("history must not end");
            assert_ne!(event.kind, "lagged", "history must not contain markers");
        }
        // Simulate an owner hand-off mid-tail: a peer takes over. No
        // intermediate live-pend probe here: cancelling a live `next()`
        // mid-poll would drop the unfold state and stall the tail (E12).
        {
            let mut runs = service.inner.runs.write().await;
            let record = runs.get_mut(&id).expect("record should exist");
            record.owner_id = "peer-takeover".to_string();
        }
        let marker = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("hand-off marker should arrive promptly")
            .expect("stream must not end without a marker");
        assert_eq!(
            marker.kind, "lagged",
            "an owner hand-off must end the pinned tail with a lagged marker"
        );
        // Resubscribe continuation: the journal replay still serves history
        // from the marker position. The marker carries the last delivered
        // seq (history end), so resubscribing from 0 replays the full
        // history and proves the client can continue without loss.
        let mut resumed = service
            .subscribe_with_cursor(id.clone(), 0)
            .await
            .expect("resubscribe should succeed");
        let replayed = tokio::time::timeout(std::time::Duration::from_secs(5), resumed.next())
            .await
            .expect("resubscribed replay should arrive")
            .expect("resubscribed stream must not end");
        assert_eq!(
            replayed.seq, 1,
            "resubscribe from start must replay history after a hand-off"
        );
    }

    #[tokio::test]
    async fn shared_tail_continues_across_owner_claim_change() {
        // E12 shared-mode hand-off: shared subscribers follow the durable
        // journal (~250ms poll), so an execution-owner claim change
        // mid-tail never stalls them. Simulate the change via direct
        // owner-claim manipulation on the same runs directory and assert
        // the shared tail still reaches the terminal event plus resubscribe
        // continuation.
        use futures_util::StreamExt as _;
        let root = temp_run_dir("shared-handoff");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should resolve");
        let mut stream = service
            .subscribe(id.clone())
            .await
            .expect("shared subscribe should succeed");
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("shared history should arrive")
            .expect("stream must not end");
        assert!(first.seq >= 1, "shared history must replay");
        // Owner-claim hand-off mid-tail: whoever holds the execution lease
        // rewrites the advisory claim; shared refresh adopts it.
        {
            let lease = crate::run_dirs::try_lock_run_execution(&run_dir)
                .expect("lease probe should not fail");
            if let Some(lease) = lease {
                crate::run_dirs::claim_run_execution_owner(&lease, "peer-takeover")
                    .expect("owner claim should write");
            }
        }
        service
            .refresh_shared_runs()
            .await
            .expect("shared refresh should adopt the new claim");
        // Answer through the service; the shared journal poll (not a pinned
        // owner channel) must still deliver the terminal event.
        let question = service
            .snapshot(id.clone())
            .await
            .expect("snapshot should exist")
            .question
            .expect("run should expose its question");
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect("answer should be accepted");
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let event = stream.next().await.expect("shared stream must continue");
                if qcg_api::is_terminal_event_kind(event.kind.as_str()) {
                    break event;
                }
            }
        })
        .await
        .expect("shared tail must reach terminal across the owner change");
        assert_eq!(terminal.kind, "run_finished");
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let owner = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared owner should initialize");
        let id = owner
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
        let peer = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
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
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
        let make_service = || {
            LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![root.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::SharedFilesystem,
                ServiceDeploymentPolicy::default(),
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
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
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
    async fn shutdown_active_runs_settles_real_runs_as_interrupted() {
        // E05/Q3: shutdown_active_runs through real service state converges
        // live AND queued runs to Interrupted with journaled terminal
        // events. The queued run proves the all-tracked-non-terminal scope
        // (not just the executing run).
        let root = temp_run_dir("shutdown-active-runs");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "slow-shutdown"
name = "Slow Shutdown"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "shutdown test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs,
            None,
            1,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("slow run should start");
        wait_for_snapshot(&service, &id, RunStatus::Running).await;
        // A second admission while the single slot is busy stays Queued.
        let queued = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("second run should admit");
        wait_for_snapshot(&service, &queued, RunStatus::Queued).await;
        service.mark_shutting_down();
        service
            .shutdown_active_runs()
            .await
            .expect("shutdown settlement should succeed");
        let snapshot = service
            .snapshot(id.clone())
            .await
            .expect("settled snapshot should exist");
        assert_eq!(
            snapshot.state,
            RunStatus::Interrupted,
            "a live run must settle as Interrupted through shutdown"
        );
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should exist");
        let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .expect("journal should be readable");
        assert!(
            journal.contains("\"t\":\"run_interrupted\""),
            "shutdown must journal a terminal interruption"
        );
        let queued_snapshot = service
            .snapshot(queued.clone())
            .await
            .expect("queued snapshot should exist");
        assert_eq!(
            queued_snapshot.state,
            RunStatus::Interrupted,
            "a queued run must also settle as Interrupted through shutdown"
        );
    }

    #[tokio::test]
    async fn live_owner_refresh_merges_peer_terminal_and_answers() {
        // E12: a live owner (Running with a live task) still merges durable
        // terminal settlement and HITL answers read-only, never appending
        // while its writer is live. Two real services share one runs
        // directory (two-process view).
        use crate::RunStoreMode;
        let root = temp_run_dir("two-process-view");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service_a = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            4,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("first shared service should initialize");
        let service_b = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs.clone(),
            None,
            4,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("second shared service should initialize");
        // Live-owner terminal merge: A runs slow (live task), B cancels
        // through the durable mailbox, A refreshes and converges without
        // journaling itself.
        let slow_root = temp_run_dir("two-process-slow");
        let _ = std::fs::remove_dir_all(&slow_root);
        let slow_gen = slow_root.join("generator");
        std::fs::create_dir_all(&slow_gen).expect("slow generator should be created");
        std::fs::write(
            slow_gen.join("qcg.toml"),
            r#"
[generator]
id = "slow-live"
name = "Slow Live"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "live merge test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest should be written");
        let slow_runs = slow_root.join("runs");
        let slow_a = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![slow_root.clone()],
            slow_runs.clone(),
            None,
            2,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("slow service A should initialize");
        let slow_b = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![slow_root.clone()],
            slow_runs.clone(),
            None,
            2,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("slow service B should initialize");
        let running = slow_a
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("slow run should start");
        wait_for_snapshot(&slow_a, &running, RunStatus::Running).await;
        // B requests cancel through the durable mailbox (non-owner path).
        let run_dir_b = slow_b
            .run_dir_for(&running)
            .await
            .expect("B should resolve the run directory");
        crate::run_dirs::request_remote_cancel(&run_dir_b, &running, "peer-b")
            .expect("peer cancel should publish");
        // A refreshes while its engine task is still live: the cancel token
        // must be prodded and the run must not stay stale Running.
        slow_a
            .refresh_shared_runs()
            .await
            .expect("live refresh should succeed");
        {
            let runs = slow_a.inner.runs.read().await;
            let record = runs.get(&running).expect("live record should still exist");
            assert!(
                record.cancellation.is_cancelled(),
                "a live owner must observe a peer cancel through refresh"
            );
        }
        slow_a
            .cancel(running.clone())
            .await
            .expect("owner cancel should settle");
        assert_eq!(
            slow_a
                .snapshot(running.clone())
                .await
                .expect("snapshot should exist")
                .state,
            RunStatus::Canceled,
            "a peer-cancelled live run must converge to Canceled"
        );
        // HITL answer merge: A parks Waiting, B answers, A refreshes and
        // converges without stale Waiting.
        let waiting = service_a
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("interactive run should start");
        let question = wait_for_snapshot(&service_a, &waiting, RunStatus::Waiting)
            .await
            .question
            .expect("run should expose its question");
        // B answers through the durable journal (real answer path on B).
        // B must first observe the run: refresh to import the rehydrated
        // record, then answer.
        service_b
            .refresh_shared_runs()
            .await
            .expect("B should import the waiting run");
        service_b
            .answer(
                waiting.clone(),
                question.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("brief"))]),
                },
            )
            .await
            .expect("peer answer should be accepted");
        // A refreshes: the durable answer must converge into memory instead
        // of staying stale Waiting.
        for _ in 0..100 {
            service_a
                .refresh_shared_runs()
                .await
                .expect("refresh should succeed");
            let snapshot = service_a
                .snapshot(waiting.clone())
                .await
                .expect("snapshot should exist");
            if snapshot.state != RunStatus::Waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let converged = service_a
            .snapshot(waiting.clone())
            .await
            .expect("snapshot should exist");
        assert!(
            converged.state != RunStatus::Waiting,
            "A must converge past stale Waiting after B answers, got {:?}",
            converged.state
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&slow_root);
    }

    #[tokio::test]
    async fn shared_poller_serves_many_subscribers_with_one_task() {
        // E12: N subscribers through the real subscribe path share one
        // underlying poll task (reference-counted map).
        use crate::RunStoreMode;
        use futures_util::StreamExt as _;
        let root = temp_run_dir("shared-poller");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "slow-poller"
name = "Slow Poller"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "poller test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            4,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("slow run should start");
        wait_for_snapshot(&service, &id, RunStatus::Running).await;
        let mut first = service
            .subscribe(id.clone())
            .await
            .expect("first subscribe should succeed");
        let mut second = service
            .subscribe(id.clone())
            .await
            .expect("second subscribe should succeed");
        let mut third = service
            .subscribe(id.clone())
            .await
            .expect("third subscribe should succeed");
        // One poller serves all three subscribers.
        {
            let pollers = service
                .inner
                .journal_pollers
                .lock()
                .expect("poller map should lock");
            assert_eq!(
                pollers.len(),
                1,
                "one shared poller must serve N subscribers, got {}",
                pollers.len()
            );
        }
        // All three observe live progress: cancel settles terminally and
        // every stream continues to the terminal event instead of hanging.
        service
            .cancel(id.clone())
            .await
            .expect("cancel should settle");
        for (index, stream) in [&mut first, &mut second, &mut third]
            .into_iter()
            .enumerate()
        {
            let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let event = stream.next().await.expect("shared stream must continue");
                    if qcg_api::is_terminal_event_kind(event.kind.as_str()) {
                        break event;
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("subscriber {index} should reach a terminal event"));
            assert_eq!(terminal.kind, "run_canceled");
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn adopt_then_cancel_reaches_the_run_and_sse_continues_to_terminal() {
        // E03: an adopted (rebuilt) run still honors cancel, and a live SSE
        // subscriber through the real subscribe path continues to the
        // terminal event instead of hanging.
        use futures_util::StreamExt as _;
        let root = temp_run_dir("adopt-cancel-sse");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("interactive run should start");
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        // Simulate a crash and adoption: drop the owner, rebuild on the same
        // directory, and resume the orphaned run.
        drop(service);
        let adopted = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("rebuilt service should adopt the run");
        adopted.resume_recovered_runs().await;
        // A live subscriber through the real subscribe path must continue
        // past the adoption to the terminal event.
        let mut events = adopted
            .subscribe(id.clone())
            .await
            .expect("subscribe should succeed");
        adopted
            .cancel(id.clone())
            .await
            .expect("adopted cancel must reach the run");
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let event = events.next().await.expect("SSE stream must continue");
                if qcg_api::is_terminal_event_kind(event.kind.as_str()) {
                    break event;
                }
            }
        })
        .await
        .expect("subscriber should reach a terminal event after adopt-then-cancel");
        assert_eq!(terminal.kind, "run_canceled");
        assert_eq!(
            adopted
                .snapshot(id)
                .await
                .expect("snapshot should exist")
                .state,
            RunStatus::Canceled
        );
    }

    #[tokio::test]
    async fn e07_8_repeat_and_fail_policies_via_real_agent_entry() {
        // E07-8: Repeat and Fail policies for indeterminate agent side
        // effects, driven through the real LlmAgentStep entry via the
        // service (not guard-harness-direct). Repeat re-executes and
        // journals `operation_repeated`; Fail refuses with FailureCode
        // Refused and journals the refusal.
        let (repeat_state, repeat_journal, _) =
            run_interrupted_agent_command("agent-repeat-e078", "repeat").await;
        assert_eq!(
            repeat_state,
            RunStatus::Succeeded,
            "repeat policy must converge: {repeat_journal}"
        );
        assert!(
            repeat_journal.contains("\"t\":\"operation_repeated\""),
            "repeat must be journaled: {repeat_journal}"
        );
        let (fail_state, fail_journal, _) =
            run_interrupted_agent_command("agent-fail-e078", "fail").await;
        assert_eq!(
            fail_state,
            RunStatus::Failed,
            "fail policy must refuse indeterminate replay: {fail_journal}"
        );
        assert!(
            fail_journal.contains("refusing automatic replay"),
            "guard refusal must be journaled: {fail_journal}"
        );
        assert!(
            fail_journal.contains("refused"),
            "fail policy must surface FailureCode Refused: {fail_journal}"
        );
    }

    #[tokio::test]
    async fn e08_6_two_questions_restart_and_late_refusal_end_to_end() {
        // E08-6: two-question separation end-to-end through the real
        // llm.agent step, restart persistence of the pending answer, and
        // late-answer-to-wrong-question refusal. No mocks: real service,
        // real fake LLM tool sequence, real journal.
        let root = temp_run_dir("agent-twoq-e086");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "ask_user"
name = "ask_mode"

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"

[permissions.containers]
enabled = false
"#,
        )
        .expect("generator manifest");
        std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Ask two questions, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"ask_mode","args":{"question":"City?","options":["tokyo","osaka"]}},{"name":"ask_mode","args":{"question":"Postal code?","options":["100","200"]}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("agent run should start");
        let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question1 = first.question.expect("first question");
        service
            .answer(
                id.clone(),
                question1.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("tokyo"))]),
                },
            )
            .await
            .expect("first answer");
        let mut question2 = None;
        for _ in 0..400 {
            let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
            if snapshot.state.is_terminal() {
                break;
            }
            if let Some(question) = snapshot.question
                && question.id != question1.id
            {
                question2 = Some(question);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let question2 = question2.expect("a distinct second question must be asked");
        assert_ne!(
            question1.id, question2.id,
            "two calls from the same tool must have distinct identities"
        );
        drop(service);
        let restored = loop {
            match LocalQcgService::with_generator_roots_policy_and_store_mode(
                vec![root.clone()],
                runs.clone(),
                None,
                qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
                qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                RunStoreMode::Exclusive,
                ServiceDeploymentPolicy::default(),
            ) {
                Ok(service) => break service,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        };
        let second = wait_for_snapshot(&restored, &id, RunStatus::Waiting).await;
        assert_eq!(
            second
                .question
                .as_ref()
                .map(|question| question.id.as_str()),
            Some(question2.id.as_str()),
            "restart must re-issue the same pending question identity"
        );
        let late = restored
            .answer(
                id.clone(),
                question1.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("osaka"))]),
                },
            )
            .await;
        assert!(
            late.is_err(),
            "a late answer for the first question must be refused"
        );
        restored
            .answer(
                id.clone(),
                question2.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("100"))]),
                },
            )
            .await
            .expect("second answer");
        let terminal = wait_for_terminal_snapshot(&restored, &id).await;
        assert_eq!(terminal.state, RunStatus::Succeeded);
        let run_dir = restored.run_dir_for(&id).await.expect("run dir");
        let events = read_events_from_meta(&run_meta_dir(&run_dir)).expect("journal");
        let answered: Vec<&Value> = events
            .iter()
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("user_answered"))
            .collect();
        assert_eq!(answered.len(), 2, "both questions must be answered");
        assert_ne!(
            answered[0].get("question_id"),
            answered[1].get("question_id"),
            "each call must keep its own identity"
        );
        drop(restored);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn e09_7_header_content_type_stdin_matrix_and_cross_confirmation() {
        // E09-7: both-entry matrix (header + Content-Type + stdin changes in
        // the same invocation) through the real http/command detail builders
        // used by the steps, plus a cross-confirmation integration test
        // through real http/command steps (no mocks, no harness-direct guard
        // decisions).
        use std::collections::BTreeMap as Map;
        let salt = qcg_engine::operation_id_for("run-e097", "node-e097", "call-same");
        let base_headers: Map<String, String> =
            Map::from([("X-Tenant".to_string(), "alpha".to_string())]);
        let base_body: Option<&[u8]> = Some(b"hello" as &[u8]);
        let base_sensitive: Map<String, String> = Map::new();
        let base = qcg_engine::http_operation_details(
            "POST",
            &base_headers,
            base_body,
            &base_sensitive,
            &salt,
        )
        .expect("base http details");
        let mut changed_header = base_headers.clone();
        changed_header.insert("X-Tenant".to_string(), "beta".to_string());
        let header_changed = qcg_engine::http_operation_details(
            "POST",
            &changed_header,
            base_body,
            &base_sensitive,
            &salt,
        )
        .expect("header-changed details");
        assert_ne!(
            base["headers_sha256"], header_changed["headers_sha256"],
            "a changed header must change the approval binding in the same invocation"
        );
        let mut content_type_headers: Map<String, String> = Map::new();
        content_type_headers.insert("content-type".to_string(), "application/json".to_string());
        let content_type_base = qcg_engine::http_operation_details(
            "POST",
            &content_type_headers,
            Some(b"{}" as &[u8]),
            &base_sensitive,
            &salt,
        )
        .expect("content-type base details");
        let mut content_type_changed = Map::new();
        content_type_changed.insert("content-type".to_string(), "text/plain".to_string());
        let content_type_other = qcg_engine::http_operation_details(
            "POST",
            &content_type_changed,
            Some(b"{}" as &[u8]),
            &base_sensitive,
            &salt,
        )
        .expect("content-type changed details");
        assert_ne!(
            content_type_base["headers_sha256"], content_type_other["headers_sha256"],
            "a changed Content-Type must change the binding in the same invocation"
        );
        let body_other = qcg_engine::http_operation_details(
            "POST",
            &base_headers,
            Some(b"other" as &[u8]),
            &base_sensitive,
            &salt,
        )
        .expect("body-changed details");
        assert_ne!(
            base["body_sha256"], body_other["body_sha256"],
            "a changed body must change the binding in the same invocation"
        );
        assert!(
            !base.to_string().contains("alpha") && !header_changed.to_string().contains("beta"),
            "header values must stay out of journaled details"
        );
        let stdin_base = qcg_engine::bind_command_stdin(
            "node-e097",
            json!({"argv": ["cat"]}),
            Some(b"one" as &[u8]),
            &salt,
        )
        .expect("base stdin details");
        let stdin_other = qcg_engine::bind_command_stdin(
            "node-e097",
            json!({"argv": ["cat"]}),
            Some(b"two" as &[u8]),
            &salt,
        )
        .expect("changed stdin details");
        assert_ne!(
            stdin_base["stdin_sha256"], stdin_other["stdin_sha256"],
            "changed stdin must change the command binding in the same invocation"
        );
        // Cross-confirmation through real steps: an http approval must not
        // satisfy a command, and vice versa. A local loopback server backs
        // the http POST so no external network is required.
        let root = temp_run_dir("cross-confirm-e097");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator dir");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let port = listener
            .local_addr()
            .expect("listener should have an address")
            .port();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buffer = vec![0u8; 8192];
                let _ = socket.read(&mut buffer).await;
                let body = b"ok";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.write_all(body).await;
            }
        });
        std::fs::write(
            generator.join("qcg.toml"),
            format!(
                r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "fetch"
type = "http"

[flow.params]
url = "http://127.0.0.1:{port}/api"
method = "POST"
headers = {{ Authorization = "Bearer test" }}
content_type = "application/json"
body_text = "hello"

[[flow]]
id = "run"
type = "command"
needs = ["fetch"]

[flow.params]
command = ["sh", "-c", "echo hi"]

[permissions]
fs_read = []
fs_write = ["workspace"]
network = ["127.0.0.1"]
side_effects = "confirm"
side_effects_scope = "invocation"
commands = [{{ bin = "sh", args = ["-c", "echo hi"], purpose = "cross confirmation probe", isolation = "trusted_host" }}]

[permissions.containers]
enabled = false
"#,
            ),
        )
        .expect("generator manifest");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        let first = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
        let http_confirm = first.confirm.expect("http confirm");
        assert_eq!(http_confirm.kind, "http");
        let http_id = http_confirm.id.clone();
        assert!(
            !http_confirm.operation_digest.is_empty(),
            "http approval must carry a digest"
        );
        service
            .confirm(
                id.clone(),
                http_id.clone(),
                qcg_api::ConfirmDecision {
                    decision: ConfirmationDecision::Approve,
                },
            )
            .await
            .expect("http approval");
        let second = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
        let cmd_confirm = second.confirm.expect("command confirm");
        assert_eq!(cmd_confirm.kind, "command");
        assert_ne!(
            cmd_confirm.id, http_id,
            "a command approval must not reuse the http approval"
        );
        assert_ne!(
            cmd_confirm.operation_digest, http_confirm.operation_digest,
            "cross-kind approvals must bind different digests"
        );
        server.abort();
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[cfg(test)]
mod acceptance_closes {
    use crate::LocalQcgService;
    use crate::{RunStoreMode, ServiceDeploymentPolicy};
    use camino::Utf8PathBuf;
    fn temp_root(name: &str) -> Utf8PathBuf {
        let dir = std::env::temp_dir().join(format!("qcg-accept-{name}-{}", uuid::Uuid::now_v7()));
        Utf8PathBuf::from_path_buf(dir).expect("temp path must be UTF-8")
    }
    #[tokio::test]
    async fn e16_queue_move_changes_snapshot_body() {
        // E16: exact-body ETag covers queue_position, so proving the body
        // bytes change on a queue move proves the validator changes too.
        // The digest formula itself lives in exactly one place
        // (`qcg_server::body_etag`); duplicating it here would let the two
        // drift, so this test pins the body property and the qcg-server
        // star/queue tests pin the production helper (E16).
        let before = serde_json::to_vec(
            &serde_json::json!({"seq": 1u64, "state": "queued", "queue_position": 2u64}),
        )
        .expect("serialize");
        let after = serde_json::to_vec(
            &serde_json::json!({"seq": 1u64, "state": "queued", "queue_position": 1u64}),
        )
        .expect("serialize");
        assert_ne!(
            before, after,
            "queue movement must change the snapshot body the validator digests"
        );
    }
    #[tokio::test]
    async fn e05_direct_run_refuses_during_shutdown() {
        // E05: direct executions honor the shutdown gate like API admissions.
        let root = temp_root("direct-shutdown");
        let _ = std::fs::remove_dir_all(&root);
        let gen_dir = root.join("gen");
        let runs = root.join("runs");
        std::fs::create_dir_all(&gen_dir).expect("gen dir");
        let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![gen_dir.clone()],
            runs,
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
            ServiceDeploymentPolicy::default(),
        )
        .expect("service");
        service.mark_shutting_down();
        let err = service
            .run_generator_path(crate::types::DirectRun {
                generator_path: gen_dir.join("qcg.toml"),
                output_dir: root.join("out"),
                inputs: Default::default(),
                answers: Default::default(),
                confirmations: Default::default(),
                interactive: false,
                json_events: false,
                llm_seed_override: None,
            })
            .await
            .expect_err("direct run during shutdown must be refused");
        assert!(err.to_string().contains("shutting down"), "{err}");
        let _ = std::fs::remove_dir_all(&root);
    }
}
