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
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).expect("zip archive should parse");
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
