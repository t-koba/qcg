mod collect;
mod manifest;
mod resolve;

pub use collect::*;
pub use manifest::*;
pub use resolve::*;

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use qcg_contract::RuntimeLimits;
    use qcg_types::{OutputArtifact, OutputManifest};

    fn temp_workspace() -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-artifact-test"))
            .expect("temporary directory path must be utf-8")
    }

    #[test]
    fn resolve_artifact_path_rejects_escape() {
        let workspace = temp_workspace();
        assert!(resolve_artifact_path(&workspace, "../secret").is_err());
        assert!(resolve_artifact_path(&workspace, "/tmp/secret").is_err());
    }

    #[test]
    fn artifact_glob_distinguishes_segment_and_recursive_wildcards() {
        assert!(glob_matches(b"reports/*.json", b"reports/one.json").unwrap());
        assert!(!glob_matches(b"reports/*.json", b"reports/archive/one.json").unwrap());
        assert!(glob_matches(b"reports/**/*.json", b"reports/archive/one.json").unwrap());
        assert!(glob_matches(b"reports/?.json", b"reports/a.json").unwrap());
        assert!(!glob_matches(b"reports/?.json", b"reports/ab.json").unwrap());
    }

    #[test]
    fn artifact_glob_rejects_oversized_patterns_and_state_spaces() {
        let oversized_pattern = vec![b'a'; MAX_ARTIFACT_GLOB_BYTES + 1];
        let workspace = temp_workspace();
        let error = matching_files(
            &workspace,
            std::str::from_utf8(&oversized_pattern).expect("ASCII pattern"),
            Some(1),
            Some(1),
        )
        .expect_err("oversized artifact glob must fail before scanning");
        assert!(
            error.to_string().contains("artifact glob exceeds"),
            "{error}"
        );

        let pattern = vec![b'?'; 2_048];
        let path = vec![b'a'; 2_048];
        let error = glob_matches(&pattern, &path)
            .expect_err("oversized artifact glob state space must fail before allocation");
        assert!(
            error.to_string().contains("glob matching exceeds"),
            "{error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_artifact_path_rejects_symlink_escape() {
        let workspace = temp_workspace().join(format!("symlink-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let outside = workspace
            .parent()
            .expect("workspace should have a parent")
            .join("outside-artifact.txt");
        std::fs::write(&outside, "outside").expect("outside file should be written");
        std::os::unix::fs::symlink(&outside, workspace.join("escaped.txt"))
            .expect("symlink should be created");
        let error = resolve_artifact_path(&workspace, "escaped.txt")
            .expect_err("symlink escape must be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn output_manifest_round_trips() {
        let workspace = temp_workspace();
        std::fs::create_dir_all(&workspace).unwrap();
        let manifest = OutputManifest {
            artifacts: vec![OutputArtifact {
                path: "done.txt".into(),
                sha256: "abc".into(),
                bytes: 3,
                label: "Done".into(),
                required: true,
                mime: Some("text/plain".into()),
                description: "Generated text".into(),
                preview: qcg_types::ArtifactPreview::Text,
            }],
        };
        write_output_manifest(&workspace, &manifest).unwrap();
        let loaded = read_output_manifest(&workspace).unwrap();
        assert_eq!(loaded.artifacts[0].path, "done.txt");
        assert_eq!(loaded.artifacts[0].mime.as_deref(), Some("text/plain"));
    }

    #[test]
    fn output_manifest_limits_reject_oversized_artifacts_before_writing() {
        let workspace = temp_workspace().join(format!("limits-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let manifest = OutputManifest {
            artifacts: vec![OutputArtifact {
                path: "large.bin".into(),
                sha256: "abc".into(),
                bytes: 3,
                label: "Large".into(),
                required: true,
                mime: None,
                description: String::new(),
                preview: qcg_types::ArtifactPreview::None,
            }],
        };
        let runtime = RuntimeLimits {
            output_file_limit_bytes: Some(2),
            ..RuntimeLimits::default()
        };
        let error = write_output_manifest_with_limits(&workspace, &manifest, &runtime)
            .expect_err("oversized output artifact must be rejected");
        assert!(error.to_string().contains("output artifact `large.bin`"));
        assert!(!workspace.join("outputs.json").exists());
    }
}
