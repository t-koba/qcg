use super::support::*;
use crate::*;
use serde_json::json;
use std::io::Cursor;

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
    assert!(terminal.state.is_terminal());
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
