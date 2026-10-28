use super::support::*;
use crate::*;
use serde_json::json;
use std::io::Cursor;

fn write_running_run(runs: &camino::Utf8Path, id: &str) -> camino::Utf8PathBuf {
    let directory = runs.join(id);
    let meta = run_meta_dir(&directory);
    std::fs::create_dir_all(&meta).expect("run metadata should be created");
    engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None)
        .expect("journal should be created")
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
                "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            }),
        )
        .expect("run should be active");
    directory
}

#[tokio::test]
async fn snapshots_read_journal_and_manifest_across_a_forced_publication() {
    for endpoint in ["snapshot", "snapshot_json", "bundle"] {
        let root = temp_run_dir(&format!("manifest-order-{endpoint}"));
        std::fs::create_dir_all(&root).expect("test root should be created");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let generators =
            camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = test_service(vec![generators], runs.clone());
        let id = format!("manifest-order-{endpoint}");
        let directory = write_running_run(&runs, &id);
        crate::tests::terminal_manifest::arm(&directory);
        let snapshot = match endpoint {
            "snapshot" => service.snapshot(id).await.expect("snapshot should succeed"),
            "snapshot_json" => serde_json::from_slice(
                &service
                    .snapshot_json(id, &ArtifactZipLimits::default())
                    .await
                    .expect("snapshot JSON should succeed"),
            )
            .expect("snapshot JSON should decode"),
            _ => {
                let parts = service
                    .run_bundle_parts(&id, &ArtifactZipLimits::default())
                    .await
                    .expect("bundle preparation should succeed");
                assert!(parts.outputs_bytes.is_some(), "manifest should be included");
                serde_json::from_slice(&parts.snapshot_bytes)
                    .expect("bundle snapshot should decode")
            }
        };
        assert_eq!(snapshot.state, api::RunStatus::Queued, "{endpoint}");
        assert!(
            snapshot.artifacts.is_some(),
            "{endpoint} should include the published manifest"
        );
    }
}

#[test]
fn artifact_zip_rejects_sources_above_limit() {
    let run_dir = temp_run_dir("zip-limit");
    let _ = std::fs::remove_dir_all(&run_dir);
    std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
    let payload = vec![b'x'; 2048];
    std::fs::write(run_workspace_dir(&run_dir).join("large.txt"), &payload)
        .expect("artifact should be written");
    std::fs::write(
        run_meta_dir(&run_dir).join("outputs.json"),
        serde_json::to_string(&json!({
            "artifacts": [{
                "path": "large.txt",
                "sha256": "unused",
                "bytes": 2048,
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
            max_bytes: Some(1024),
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
    let mut payload = vec![0u8; 4096];
    let mut state: u32 = 0x12345678;
    for byte in payload.iter_mut() {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        *byte = (state >> 16) as u8;
    }
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(&payload))
    };
    std::fs::write(run_workspace_dir(&run_dir).join("a.bin"), &payload)
        .expect("artifact should be written");
    std::fs::write(
        run_meta_dir(&run_dir).join("outputs.json"),
        serde_json::to_string(&json!({
            "artifacts": [{
                "path": "a.bin",
                "sha256": digest,
                "bytes": 4096,
                "label": "A",
                "required": true
            }]
        }))
        .expect("manifest should serialize"),
    )
    .expect("manifest should be written");
    let full = read_artifacts_zip_with_limits(
        &run_dir,
        &ArtifactZipLimits {
            max_bytes: None,
            max_entries: None,
        },
    )
    .expect("full zip should succeed");
    assert!(
        full.len() > 1024,
        "incompressible fixture must exceed raw bound"
    );
    let mut bytes = Cursor::new(Vec::new());
    let error = write_artifacts_zip_stream_with_limits(
        &run_dir,
        &mut bytes,
        &ArtifactZipLimits {
            max_bytes: Some((full.len() - 1) as u64),
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
            max_bytes: Some(2048),
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
            max_bytes: Some(2048),
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

#[test]
fn artifact_zip_contains_manifest_artifacts() {
    let run_dir = temp_run_dir("zip-file");
    let _ = std::fs::remove_dir_all(&run_dir);
    std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
    std::fs::create_dir_all(run_workspace_dir(&run_dir).join("reports"))
        .expect("artifact directory should be written");
    let report_dir = run_workspace_dir(&run_dir).join("reports");
    let artifact_path = report_dir.join("result.txt");
    std::fs::write(&artifact_path, "ok").expect("artifact should be written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&report_dir, std::fs::Permissions::from_mode(0o755))
            .expect("artifact directory mode should be set");
        std::fs::set_permissions(&artifact_path, std::fs::Permissions::from_mode(0o644))
            .expect("artifact file mode should be set");
    }
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
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).expect("zip archive should parse");
    assert!(
        archive
            .by_name("reports/")
            .expect("directory entry")
            .is_dir()
    );
    assert_eq!(
        archive
            .by_name("reports/")
            .expect("directory mode entry")
            .unix_mode()
            .expect("directory permissions")
            & 0o777,
        0o755
    );
    let mut entry = archive
        .by_name("reports/result.txt")
        .expect("artifact should be present");
    assert_eq!(
        entry.unix_mode().expect("artifact permissions") & 0o777,
        0o644
    );
    assert!(
        entry.last_modified().expect("artifact timestamp").year() > 1980,
        "artifact timestamp must come from source metadata"
    );
    let mut text = String::new();
    std::io::Read::read_to_string(&mut entry, &mut text).expect("artifact should read as text");
    assert_eq!(text, "ok");
    let _ = std::fs::remove_dir_all(&run_dir);
}

#[test]
fn dedicated_raw_manifest_bound_rejects_before_deserialization() {
    let run_dir = temp_run_dir("dedicated-raw-bound");
    let _ = std::fs::remove_dir_all(&run_dir);
    std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace");
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("meta");
    std::fs::write(run_workspace_dir(&run_dir).join("ok.txt"), "ok").expect("artifact");
    let manifest = serde_json::to_string(&json!({
        "artifacts": [{
            "path": "ok.txt",
            "sha256": "2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df",
            "bytes": 2,
            "label": "Ok",
            "required": true
        }]
    }))
    .expect("manifest");
    std::fs::write(run_meta_dir(&run_dir).join("outputs.json"), &manifest).expect("manifest write");
    let raw_len = manifest.len();
    let ok = crate::artifacts::collect_verified_with_limits(
        &run_dir,
        &ArtifactZipLimits {
            max_bytes: Some(raw_len as u64),
            max_entries: None,
        },
    )
    .expect("at-limit raw must pass");
    assert_eq!(ok.len(), 1);
    let err = crate::artifacts::collect_verified_with_limits(
        &run_dir,
        &ArtifactZipLimits {
            max_bytes: Some((raw_len - 1) as u64),
            max_entries: None,
        },
    )
    .expect_err("over-limit raw must fail before deserialization");
    assert!(err.to_string().contains("exceeds"), "{err}");
    let _ = std::fs::remove_dir_all(&run_dir);
}

#[test]
fn dedicated_nested_zip_directories_count_toward_entry_cap() {
    let run_dir = temp_run_dir("dedicated-nested-entries");
    let _ = std::fs::remove_dir_all(&run_dir);
    std::fs::create_dir_all(run_workspace_dir(&run_dir).join("a/b")).expect("nested dirs");
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("meta");
    std::fs::write(run_workspace_dir(&run_dir).join("a/b/c.txt"), "hi").expect("artifact");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(
            run_workspace_dir(&run_dir).join("a"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("mode");
        std::fs::set_permissions(
            run_workspace_dir(&run_dir).join("a/b"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("mode");
        std::fs::set_permissions(
            run_workspace_dir(&run_dir).join("a/b/c.txt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("mode");
    }
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(b"hi"))
    };
    std::fs::write(
        run_meta_dir(&run_dir).join("outputs.json"),
        serde_json::to_string(&json!({
            "artifacts": [{ "path": "a/b/c.txt", "sha256": digest, "bytes": 2, "label": "Nested", "required": true }]
        }))
        .expect("manifest"),
    )
    .expect("manifest write");
    let mut buf = Cursor::new(Vec::new());
    let err = write_artifacts_zip_stream_with_limits(
        &run_dir,
        &mut buf,
        &ArtifactZipLimits {
            max_bytes: None,
            max_entries: Some(2),
        },
    )
    .expect_err("a/b + a/b/c.txt is 3 entries and must exceed cap 2");
    assert!(err.to_string().contains("entries exceed limit"), "{err}");
    let mut ok_buf = Cursor::new(Vec::new());
    write_artifacts_zip_stream_with_limits(
        &run_dir,
        &mut ok_buf,
        &ArtifactZipLimits {
            max_bytes: None,
            max_entries: Some(3),
        },
    )
    .expect("at-limit 3 entries must pass");
    let _ = std::fs::remove_dir_all(&run_dir);
}

#[test]
fn dedicated_many_nested_directories_fail_incrementally() {
    let run_dir = temp_run_dir("dedicated-many-nested");
    let _ = std::fs::remove_dir_all(&run_dir);
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("meta");
    let depth = 48_usize;
    let mut manifest_path = String::from("deep");
    for _ in 1..depth {
        manifest_path.push_str("/x");
    }
    manifest_path.push_str("/file.txt");
    let disk_path = run_workspace_dir(&run_dir).join(&manifest_path);
    if let Some(parent) = disk_path.parent() {
        std::fs::create_dir_all(parent).expect("deep dirs");
    }
    std::fs::write(&disk_path, "hi").expect("artifact");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&disk_path, std::fs::Permissions::from_mode(0o644)).expect("mode");
    }
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(b"hi"))
    };
    std::fs::write(
        run_meta_dir(&run_dir).join("outputs.json"),
        serde_json::to_string(&json!({
            "artifacts": [{ "path": manifest_path, "sha256": digest, "bytes": 2, "label": "Deep", "required": true }]
        }))
        .expect("manifest"),
    )
    .expect("manifest write");
    let mut buf = Cursor::new(Vec::new());
    let err = write_artifacts_zip_stream_with_limits(
        &run_dir,
        &mut buf,
        &ArtifactZipLimits {
            max_bytes: None,
            max_entries: Some(32),
        },
    )
    .expect_err("48 nested prefixes plus file must exceed 32-entry cap");
    assert!(err.to_string().contains("entries exceed limit"), "{err}");
    assert!(
        buf.get_ref().is_empty(),
        "incremental directory cap must fire before hashing/streaming"
    );
    let _ = std::fs::remove_dir_all(&run_dir);
}

#[test]
fn dedicated_effective_defaults_and_larger_override() {
    use crate::package::{DEDICATED_ARTIFACT_MAX_BYTES, DEDICATED_ARTIFACT_MAX_ENTRIES};
    assert_eq!(DEDICATED_ARTIFACT_MAX_BYTES, 16 * 1024 * 1024);
    assert_eq!(DEDICATED_ARTIFACT_MAX_ENTRIES, 1000);
    let base = ArtifactZipLimits {
        max_bytes: None,
        max_entries: None,
    }
    .effective();
    assert_eq!(base.max_bytes, Some(DEDICATED_ARTIFACT_MAX_BYTES));
    assert_eq!(base.max_entries, Some(DEDICATED_ARTIFACT_MAX_ENTRIES));
    let larger = ArtifactZipLimits {
        max_bytes: Some(32 * 1024 * 1024),
        max_entries: Some(2000),
    }
    .effective();
    assert_eq!(larger.max_bytes, Some(32 * 1024 * 1024));
    assert_eq!(larger.max_entries, Some(2000));
    let smaller = ArtifactZipLimits {
        max_bytes: Some(1),
        max_entries: Some(1),
    }
    .effective();
    assert_eq!(smaller.max_bytes, Some(1));
    assert_eq!(smaller.max_entries, Some(1));
    assert_eq!(ArtifactZipLimits::default().max_bytes, None);
    assert_eq!(ArtifactZipLimits::default().max_entries, None);
}

#[cfg(unix)]
#[test]
fn dedicated_zip_source_growth_cannot_finish_successfully() {
    struct AppendOnWrite {
        path: std::path::PathBuf,
        output: Vec<u8>,
        changed: bool,
    }
    impl std::io::Write for AppendOnWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.changed {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&self.path)?
                    .write_all(b"x")?;
                self.changed = true;
            }
            self.output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let run_dir = temp_run_dir("dedicated-source-growth");
    let _ = std::fs::remove_dir_all(&run_dir);
    std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace");
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("meta");
    let artifact_path = run_workspace_dir(&run_dir).join("growing.txt");
    std::fs::write(&artifact_path, b"hi").expect("artifact");
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(b"hi"))
    };
    std::fs::write(
        run_meta_dir(&run_dir).join("outputs.json"),
        serde_json::to_string(&json!({
            "artifacts": [{ "path": "growing.txt", "sha256": digest, "bytes": 2, "label": "Growing", "required": true }]
        }))
        .expect("manifest"),
    )
    .expect("manifest write");
    let mut writer = AppendOnWrite {
        path: artifact_path.as_std_path().to_path_buf(),
        output: Vec::new(),
        changed: false,
    };
    let error = write_artifacts_zip_stream_with_limits(
        &run_dir,
        &mut writer,
        &ArtifactZipLimits::default(),
    )
    .expect_err("growth after hash preflight must fail the bounded copy");
    assert!(error.to_string().contains("bytes mismatch"), "{error}");
    assert!(writer.changed);
    let _ = std::fs::remove_dir_all(&run_dir);
}

#[tokio::test]
async fn snapshot_json_checks_cached_entries_and_bounds_complete_body() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("snapshot-cached-body");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let terminal = wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
        .expect("run journal should be available");
    assert_eq!(
        terminal.state,
        api::RunStatus::Succeeded,
        "run must succeed before checking cached artifacts; journal:\n{journal}"
    );
    let cached_entries = terminal
        .artifacts
        .as_ref()
        .expect("terminal run should cache manifest")
        .artifacts
        .len();
    assert!(cached_entries > 0);
    service
        .snapshot_json(
            id.clone(),
            &ArtifactZipLimits {
                max_bytes: None,
                max_entries: Some(cached_entries),
            },
        )
        .await
        .expect("exact cached entry limit must pass");
    let cached_err = service
        .snapshot_json(
            id.clone(),
            &ArtifactZipLimits {
                max_bytes: None,
                max_entries: Some(cached_entries - 1),
            },
        )
        .await
        .expect_err("cached entry overflow must fail");
    assert!(
        matches!(cached_err, api::ApiError::Invalid { .. }),
        "{cached_err}"
    );
    let body = service
        .snapshot_json(id.clone(), &ArtifactZipLimits::default())
        .await
        .expect("default snapshot body should succeed");
    assert!(!body.is_empty());
    let exact = ArtifactZipLimits {
        max_bytes: Some(body.len() as u64),
        max_entries: None,
    };
    let exact_body = service
        .snapshot_json(id.clone(), &exact)
        .await
        .expect("exact body limit must pass");
    assert_eq!(exact_body, body);
    let over = ArtifactZipLimits {
        max_bytes: Some((body.len() - 1) as u64),
        max_entries: None,
    };
    let over_err = service
        .snapshot_json(id.clone(), &over)
        .await
        .expect_err("body over limit must fail");
    assert!(
        matches!(over_err, api::ApiError::TooLarge { .. }),
        "{over_err}"
    );
    let unbounded = service
        .snapshot(id.clone())
        .await
        .expect("unbounded snapshot stays available");
    assert_eq!(unbounded.run_id, id);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn snapshot_json_bounds_disk_manifest_and_preserves_absent() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("snapshot-disk-manifest");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    {
        let mut map = service.inner.runs.write().await;
        if let Some(record) = map.get_mut(&id) {
            record.artifacts = None;
        }
    }
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let raw =
        std::fs::read(run_meta_dir(&run_dir).join("outputs.json")).expect("manifest should exist");
    assert!(!raw.is_empty());
    let disk_entries = serde_json::from_slice::<model::OutputManifest>(&raw)
        .expect("manifest JSON")
        .artifacts
        .len();
    assert!(disk_entries > 0);
    service
        .snapshot_json(
            id.clone(),
            &ArtifactZipLimits {
                max_bytes: None,
                max_entries: Some(disk_entries),
            },
        )
        .await
        .expect("exact disk entry limit must pass");
    let over_raw = ArtifactZipLimits {
        max_bytes: Some((raw.len() - 1) as u64),
        max_entries: None,
    };
    let raw_err = service
        .snapshot_json(id.clone(), &over_raw)
        .await
        .expect_err("short raw limit must fail");
    assert!(
        matches!(raw_err, api::ApiError::TooLarge { .. }),
        "{raw_err}"
    );
    let invalid_raw = vec![b'x'; 512];
    std::fs::write(run_meta_dir(&run_dir).join("outputs.json"), &invalid_raw)
        .expect("invalid manifest write");
    let short = ArtifactZipLimits {
        max_bytes: Some(511),
        max_entries: None,
    };
    let short_err = service
        .snapshot_json(id.clone(), &short)
        .await
        .expect_err("oversized invalid raw must fail before deserialization");
    assert!(
        matches!(short_err, api::ApiError::TooLarge { .. }),
        "{short_err}"
    );
    let exact_invalid = ArtifactZipLimits {
        max_bytes: Some(512),
        max_entries: None,
    };
    let json_err = service
        .snapshot_json(id.clone(), &exact_invalid)
        .await
        .expect_err("exact invalid raw must reach deserialization");
    assert!(
        matches!(json_err, api::ApiError::Internal { .. }),
        "{json_err}"
    );
    std::fs::write(run_meta_dir(&run_dir).join("outputs.json"), &raw).expect("manifest restore");
    let entry_err = service
        .snapshot_json(
            id.clone(),
            &ArtifactZipLimits {
                max_bytes: None,
                max_entries: Some(disk_entries - 1),
            },
        )
        .await
        .expect_err("disk entry overflow must fail");
    assert!(
        matches!(entry_err, api::ApiError::Invalid { .. }),
        "{entry_err}"
    );
    std::fs::remove_file(run_meta_dir(&run_dir).join("outputs.json")).expect("manifest removal");
    let absent_body = service
        .snapshot_json(id.clone(), &ArtifactZipLimits::default())
        .await
        .expect("absent manifest must stay None");
    let absent: api::RunSnapshot =
        serde_json::from_slice(&absent_body).expect("snapshot body should deserialize");
    assert!(absent.artifacts.is_none());
    let unbounded = service
        .snapshot(id.clone())
        .await
        .expect("unbounded absent stays None");
    assert!(unbounded.artifacts.is_none());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn bundle_metadata_count_reports_1048595() {
    let payload = "x".repeat(1_048_593);
    let value = serde_json::Value::String(payload);
    let count = crate::artifacts::json_size(&value).expect("nonallocating metadata count");
    assert_eq!(count, 1_048_595);
    let exact = ArtifactZipLimits {
        max_bytes: Some(count),
        max_entries: None,
    };
    crate::artifacts::check_bundle_limits(count, 0, &exact).expect("exact count passes");
    let err = crate::artifacts::check_bundle_limits(
        count,
        0,
        &ArtifactZipLimits {
            max_bytes: Some(count - 1),
            max_entries: None,
        },
    )
    .expect_err("over count must fail before buffering");
    match err {
        crate::types::ServiceError::TooLarge { actual, limit } => {
            assert_eq!(actual, 1_048_595);
            assert_eq!(limit, 1_048_594);
        }
        other => panic!("expected TooLarge, got {other}"),
    }
}

#[tokio::test]
async fn bundle_aggregate_bytes_bound() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("bundle-aggregate");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    let unlimited = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect("unbounded bundle should prepare");
    let mut full = Vec::new();
    write_run_bundle_stream_with_limits(&unlimited, &mut full, &ArtifactZipLimits::default())
        .expect("unbounded stream should succeed");
    assert!(!full.is_empty());
    let manifest_len = std::fs::metadata(
        run_meta_dir(&service.run_dir_for(&id).await.expect("run dir")).join("outputs.json"),
    )
    .expect("raw manifest length")
    .len();
    let total = manifest_len
        + unlimited.snapshot_bytes.len() as u64
        + unlimited.inputs_bytes.len() as u64
        + unlimited
            .outputs_bytes
            .as_ref()
            .map_or(0, |b| b.len() as u64)
        + unlimited.journal_len
        + unlimited.audit_len.unwrap_or(0)
        + unlimited.verified.iter().map(|a| a.bytes).sum::<u64>();
    assert_eq!(
        unlimited.source_bytes, total,
        "all preparation sources counted exactly"
    );
    let exact = ArtifactZipLimits {
        max_bytes: Some(unlimited.source_bytes),
        max_entries: None,
    };
    service
        .run_bundle_parts(&id, &exact)
        .await
        .expect("exact aggregate must pass");
    let over = ArtifactZipLimits {
        max_bytes: Some(total.saturating_sub(1)),
        max_entries: None,
    };
    let err = service
        .run_bundle_parts(&id, &over)
        .await
        .expect_err("aggregate over must fail");
    assert!(matches!(err, api::ApiError::TooLarge { .. }), "{err}");
    assert!(
        (full.len() as u64) < total,
        "compressed archive is smaller than its sources"
    );
    let mut denied = Vec::new();
    let err = write_run_bundle_stream_with_limits(&unlimited, &mut denied, &over)
        .expect_err("stricter writer must recheck aggregate even with compression");
    assert!(matches!(err, ServiceError::TooLarge { .. }), "{err}");
    assert!(denied.is_empty(), "writer preflight emits no bytes");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bundle_entry_exact_and_over_with_empty_audit() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("bundle-empty-audit");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    std::fs::write(run_meta_dir(&run_dir).join("audit.jsonl"), "").expect("empty audit present");
    let prepared = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect("empty audit should prepare");
    assert_eq!(
        prepared.audit_len,
        Some(0),
        "empty present audit must pin Some(0)"
    );
    let entries = 2
        + usize::from(prepared.outputs_bytes.is_some())
        + 1
        + 1
        + prepared.directories.len()
        + prepared.verified.len();
    let exact = ArtifactZipLimits {
        max_bytes: None,
        max_entries: Some(entries),
    };
    service
        .run_bundle_parts(&id, &exact)
        .await
        .expect("exact entry with empty audit must pass");
    let over = ArtifactZipLimits {
        max_bytes: None,
        max_entries: Some(entries - 1),
    };
    let err = service
        .run_bundle_parts(&id, &over)
        .await
        .expect_err("entry over with empty audit must fail");
    assert!(matches!(err, api::ApiError::Invalid { .. }), "{err}");
    let mut denied = Vec::new();
    let err = write_run_bundle_stream_with_limits(&prepared, &mut denied, &over)
        .expect_err("stricter writer must recheck directory-inclusive entries");
    assert!(matches!(err, ServiceError::ArtifactEntries), "{err}");
    assert!(denied.is_empty(), "writer preflight emits no bytes");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bundle_override_and_cli_default_preservation() {
    use crate::package::{DEDICATED_ARTIFACT_MAX_BYTES, DEDICATED_ARTIFACT_MAX_ENTRIES};
    assert_eq!(DEDICATED_ARTIFACT_MAX_BYTES, 16 * 1024 * 1024);
    assert_eq!(DEDICATED_ARTIFACT_MAX_ENTRIES, 1000);
    assert_eq!(ArtifactZipLimits::default().max_bytes, None);
    assert_eq!(ArtifactZipLimits::default().max_entries, None);
    let base = ArtifactZipLimits::default().effective();
    assert_eq!(base.max_bytes, Some(DEDICATED_ARTIFACT_MAX_BYTES));
    assert_eq!(base.max_entries, Some(DEDICATED_ARTIFACT_MAX_ENTRIES));
    let larger = ArtifactZipLimits {
        max_bytes: Some(32 * 1024 * 1024),
        max_entries: Some(2000),
    }
    .effective();
    assert_eq!(larger.max_bytes, Some(32 * 1024 * 1024));
    assert_eq!(larger.max_entries, Some(2000));
}

#[tokio::test]
async fn bundle_hash_fails_before_headers() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("bundle-hash-preheader");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let mut manifest: model::OutputManifest = serde_json::from_slice(
        &std::fs::read(run_meta_dir(&run_dir).join("outputs.json")).expect("manifest"),
    )
    .expect("manifest json");
    assert!(!manifest.artifacts.is_empty());
    let first = &mut manifest.artifacts[0];
    let fs_path = run_workspace_dir(&run_dir).join(&first.path);
    let nested = run_workspace_dir(&run_dir).join("a/b/c.txt");
    std::fs::create_dir_all(nested.parent().expect("parent")).expect("nested directory");
    std::fs::rename(&fs_path, &nested).expect("move artifact");
    first.path = "a/b/c.txt".into();
    let manifest_path = run_meta_dir(&run_dir).join("outputs.json");
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("serialize"),
    )
    .expect("manifest");
    let prepared = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect("valid nested artifact");
    // A bad digest is deliberately present: bounds must win before hash I/O.
    manifest.artifacts[0].sha256 = "0".repeat(64);
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("serialize"),
    )
    .expect("manifest");
    let bytes_err = service
        .run_bundle_parts(
            &id,
            &ArtifactZipLimits {
                max_bytes: Some(prepared.source_bytes - 1),
                max_entries: None,
            },
        )
        .await
        .expect_err("aggregate admission precedes hashes");
    assert!(
        matches!(bytes_err, api::ApiError::TooLarge { .. }),
        "{bytes_err}"
    );
    let entries_err = service
        .run_bundle_parts(
            &id,
            &ArtifactZipLimits {
                max_bytes: None,
                max_entries: Some(prepared.entry_count - 1),
            },
        )
        .await
        .expect_err("directory-inclusive admission precedes hashes");
    assert!(
        matches!(entries_err, api::ApiError::Invalid { field: Some(ref field), .. }
        if field == "artifacts"),
        "{entries_err}"
    );
    let count_err = service
        .run_bundle_parts(
            &id,
            &ArtifactZipLimits {
                max_bytes: None,
                max_entries: Some(0),
            },
        )
        .await
        .expect_err("manifest artifact count precedes hashes");
    assert!(
        matches!(count_err, api::ApiError::Invalid { .. }),
        "{count_err}"
    );
    let hash_err = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect_err("hash mismatch must fail before headers");
    assert!(
        hash_err.to_string().contains("sha256 mismatch"),
        "{hash_err}"
    );
    // The single raw parse must not admit duplicate paths, even with no limits.
    manifest.artifacts.push(manifest.artifacts[0].clone());
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("serialize"),
    )
    .expect("manifest");
    let duplicate_err = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect_err("duplicate manifest paths must fail preflight before hashing");
    assert!(
        duplicate_err.to_string().contains("duplicate artifact"),
        "{duplicate_err}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bundle_manifest_and_journal_too_large() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("bundle-too-large");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let raw = std::fs::read(run_meta_dir(&run_dir).join("outputs.json")).expect("manifest raw");
    assert!(!raw.is_empty());
    let short = ArtifactZipLimits {
        max_bytes: Some((raw.len() - 1) as u64),
        max_entries: None,
    };
    let err = service
        .run_bundle_parts(&id, &short)
        .await
        .expect_err("short manifest must be TooLarge");
    assert!(matches!(err, api::ApiError::TooLarge { .. }), "{err}");
    let j_len = std::fs::metadata(run_meta_dir(&run_dir).join("journal.jsonl"))
        .expect("journal meta")
        .len();
    assert!(
        (raw.len() as u64) < j_len - 1,
        "journal cap must admit the raw manifest"
    );
    let j_short = ArtifactZipLimits {
        max_bytes: Some(j_len - 1),
        max_entries: None,
    };
    let j_err = service
        .run_bundle_parts(&id, &j_short)
        .await
        .expect_err("short journal must be TooLarge");
    assert!(matches!(j_err, api::ApiError::TooLarge { .. }), "{j_err}");
    // Default CLI ZIP limits are unbounded, but engine journal limits remain active.
    std::fs::write(
        run_meta_dir(&run_dir).join("journal.jsonl"),
        vec![b'x'; engine::JournalLimits::default().max_event_bytes.unwrap() + 1],
    )
    .expect("oversized journal event");
    let event_err = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect_err("default journal event bound must survive unbounded ZIP policy");
    assert!(
        event_err.to_string().contains("event exceeds"),
        "{event_err}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bundle_source_growth_pinning() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    use std::io::Write as _;
    let root = temp_run_dir("bundle-growth");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), serde_json::json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    std::fs::write(run_meta_dir(&run_dir).join("audit.jsonl"), "audit0\n").expect("audit");
    // Engine scanning tolerates a torn final line and reports every byte consumed.
    let journal = run_meta_dir(&run_dir).join("journal.jsonl");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&journal)
        .expect("journal")
        .write_all(b"{\"unfinished\":")
        .expect("torn final event");
    let parts = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect("bundle should prepare");
    assert_eq!(
        parts.journal_len,
        std::fs::metadata(&journal).unwrap().len()
    );
    let mut sources = vec![
        ("journal.jsonl".to_string(), parts.journal.clone()),
        (
            "audit.jsonl".to_string(),
            parts.journal.with_file_name("audit.jsonl"),
        ),
    ];
    sources.extend(
        parts
            .verified
            .iter()
            .map(|a| (format!("artifacts/{}", a.path), a.fs_path.clone())),
    );
    let pinned: Vec<_> = sources
        .iter()
        .map(|(name, path)| (name.clone(), std::fs::read(path).expect("pinned content")))
        .collect();
    for (_, path) in &sources {
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("append source")
            .write_all(b"appended bytes must not enter ZIP")
            .expect("growth");
    }
    let mut buf = Vec::new();
    write_run_bundle_stream_with_limits(&parts, &mut buf, &ArtifactZipLimits::default())
        .expect("appends must not affect pinned streams");
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(buf)).expect("finished ZIP");
    for (name, bytes) in &pinned {
        use std::io::Read as _;
        let mut entry = zip.by_name(name).expect("source entry");
        assert_eq!(entry.size(), bytes.len() as u64, "{name}");
        let mut actual = Vec::new();
        entry.read_to_end(&mut actual).expect("read entry");
        assert_eq!(&actual, bytes, "{name} must exclude appended bytes");
    }
    // A shortened source is a real post-header I/O failure, not validation.
    for ((_, path), (_, bytes)) in sources.iter().zip(&pinned) {
        std::fs::write(path, []).expect("truncate source");
        let err =
            write_run_bundle_stream_with_limits(&parts, Vec::new(), &ArtifactZipLimits::default())
                .expect_err("short read must fail streaming");
        assert!(
            matches!(err, ServiceError::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof),
            "{err}"
        );
        std::fs::write(path, bytes).expect("restore pinned source");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn bundle_footer_cap_includes_footer() {
    let root = temp_run_dir("bundle-footer");
    std::fs::create_dir_all(&root).expect("temp dir");
    let journal = root.join("journal.jsonl");
    std::fs::write(&journal, []).expect("empty journal");
    let parts = RunBundleParts {
        source_bytes: 4,
        entry_count: 3,
        snapshot_bytes: b"{}".to_vec(),
        inputs_bytes: b"{}".to_vec(),
        outputs_bytes: None,
        journal,
        journal_len: 0,
        audit_len: None,
        verified: vec![],
        directories: vec![],
    };
    let mut full = Vec::new();
    write_run_bundle_stream_with_limits(&parts, &mut full, &ArtifactZipLimits::default())
        .expect("full bundle should succeed");
    let full_len = full.len() as u64;
    assert!(
        full_len - 1 > parts.source_bytes,
        "source admission must pass"
    );
    let mut buf = Vec::new();
    let err = write_run_bundle_stream_with_limits(
        &parts,
        &mut buf,
        &ArtifactZipLimits {
            max_bytes: Some(full_len - 1),
            max_entries: None,
        },
    )
    .expect_err("footer-inclusive cap must fail at full-1");
    assert!(err.to_string().contains("exceeds"), "{err}");
    assert_eq!(buf.len() as u64, full_len - 1);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bundle_same_length_mutation_after_preparation_aborts_archive() {
    use api::StartRun;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;
    let root = temp_run_dir("bundle-streamed-digest");
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "hello-template".into(),
            inputs: BTreeMap::from([("name".into(), json!("test"))]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_terminal_snapshot(&service, &id).await;
    let parts = service
        .run_bundle_parts(&id, &ArtifactZipLimits::default())
        .await
        .expect("preparation must verify original digest");
    let artifact = parts.verified.first().expect("generated artifact");
    let original = std::fs::read(&artifact.fs_path).expect("original content");
    assert!(!original.is_empty());
    let mut full = Vec::new();
    write_run_bundle_stream_with_limits(&parts, &mut full, &ArtifactZipLimits::default())
        .expect("unchanged prepared bundle must succeed");
    zip::ZipArchive::new(Cursor::new(full)).expect("valid unmodified archive");

    let mut changed = original.clone();
    changed[0] ^= 1;
    std::fs::write(&artifact.fs_path, &changed).expect("same-length mutation");
    assert_eq!(changed.len() as u64, artifact.bytes);
    let mut partial = Vec::new();
    let error =
        write_run_bundle_stream_with_limits(&parts, &mut partial, &ArtifactZipLimits::default())
            .expect_err("streamed content must match the prepared manifest digest");
    assert!(error.to_string().contains("sha256 mismatch"), "{error}");
    assert!(!partial.is_empty(), "failure occurs after streaming starts");
    assert!(
        zip::ZipArchive::new(Cursor::new(partial)).is_err(),
        "even ZipWriter::drop must not finalize a mismatched archive"
    );

    std::fs::write(&artifact.fs_path, original).expect("restore original");
    let mut restored = Vec::new();
    write_run_bundle_stream_with_limits(&parts, &mut restored, &ArtifactZipLimits::default())
        .expect("restored digest must succeed");
    zip::ZipArchive::new(Cursor::new(restored)).expect("valid restored archive");
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn dedicated_zip_same_length_mutation_after_preflight_aborts_archive() {
    struct MutateOnWrite {
        path: camino::Utf8PathBuf,
        output: Vec<u8>,
        changed: bool,
    }
    impl std::io::Write for MutateOnWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.changed {
                // The first ZIP header is emitted only after the hash preflight.
                std::fs::write(&self.path, b"ho")?;
                self.changed = true;
            }
            self.output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let run_dir = temp_run_dir("dedicated-streamed-digest");
    std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace");
    std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata");
    let path = run_workspace_dir(&run_dir).join("artifact.txt");
    std::fs::write(&path, b"hi").expect("original content");
    let digest = {
        use sha2::Digest as _;
        hex::encode(sha2::Sha256::digest(b"hi"))
    };
    std::fs::write(
        run_meta_dir(&run_dir).join("outputs.json"),
        serde_json::to_vec(&json!({
            "artifacts": [{"path": "artifact.txt", "sha256": digest, "bytes": 2,
                "label": "Artifact", "required": true}]
        }))
        .expect("manifest"),
    )
    .expect("write manifest");
    let full = read_artifacts_zip(&run_dir).expect("original artifact must pass");
    zip::ZipArchive::new(Cursor::new(full)).expect("valid unmodified archive");
    let mut writer = MutateOnWrite {
        path,
        output: Vec::new(),
        changed: false,
    };
    let error = write_artifacts_zip_stream_with_limits(
        &run_dir,
        &mut writer,
        &ArtifactZipLimits::default(),
    )
    .expect_err("same-length mutation after preflight must fail");
    assert!(error.to_string().contains("sha256 mismatch"), "{error}");
    assert!(writer.changed);
    assert!(!writer.output.is_empty());
    assert!(zip::ZipArchive::new(Cursor::new(writer.output)).is_err());
    let _ = std::fs::remove_dir_all(run_dir);
}
