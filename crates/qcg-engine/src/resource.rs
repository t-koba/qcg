mod content;
mod file_loaders;
mod hash;
mod remote_exec;
mod skill;
mod snapshot;
mod types;

#[cfg(test)]
pub(crate) use content::*;
#[cfg(test)]
pub(crate) use hash::*;
#[cfg(test)]
pub(crate) use snapshot::*;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use qcg_contract::ResourceDef;

    fn test_directory(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-resource-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path should be UTF-8")
    }

    #[test]
    fn directory_hash_is_streamed_sorted_and_bounded() {
        let root = test_directory("limits");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("nested")).expect("directory should be created");
        std::fs::write(root.join("z.txt"), "z").expect("file should be written");
        std::fs::write(root.join("nested/a.txt"), "abc").expect("file should be written");

        let (_, files) = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_files: Some(2),
                max_bytes: Some(4),
                ..DirectoryLimits::default()
            },
        )
        .expect("directory within limits should hash");
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["nested/a.txt", "z.txt"]
        );

        let files_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_files: Some(1),
                max_bytes: Some(4),
                ..DirectoryLimits::default()
            },
        )
        .expect_err("file limit should be enforced");
        assert!(files_error.to_string().contains("max_files"));
        let bytes_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_files: Some(2),
                max_bytes: Some(3),
                ..DirectoryLimits::default()
            },
        )
        .expect_err("byte limit should be enforced");
        assert!(bytes_error.to_string().contains("max_bytes"));
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn resource_configuration_accepts_explicit_large_limits() {
        let directory: ResourceDef = serde_json::from_value(serde_json::json!({
            "type": "dir",
            "params": { "max_files": 2_000_000 }
        }))
        .expect("directory resource should deserialize");
        directory_limits("docs", &directory).expect("explicit large limit must validate");

        let file: ResourceDef = serde_json::from_value(serde_json::json!({
            "type": "file",
            "params": { "max_bytes": 2_147_483_648u64 }
        }))
        .expect("file resource should deserialize");
        single_resource_limits("document", &file).expect("explicit large limit must validate");
    }

    #[test]
    fn directory_limits_count_empty_directories_and_bound_depth() {
        let root = test_directory("entry-depth");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("nested/empty"))
            .expect("nested empty directory should be created");
        let entry_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_entries: Some(1),
                ..DirectoryLimits::default()
            },
        )
        .expect_err("empty directories must count toward max_entries");
        assert!(entry_error.to_string().contains("max_entries"));
        let depth_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_depth: Some(1),
                ..DirectoryLimits::default()
            },
        )
        .expect_err("nested directories must respect max_depth");
        assert!(depth_error.to_string().contains("max_depth"));
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn resource_file_selector_rejects_parent_traversal() {
        let root = test_directory("selector");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("directory should be created");
        let error = resolve_resource_file("docs", &root, "../secret.txt")
            .expect_err("parent traversal should be rejected");
        assert!(matches!(error, ResourceError::UnsafeFileSelector { .. }));
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn resource_text_reads_stop_at_the_configured_bound() {
        let root = test_directory("read-bound");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("directory should be created");
        let path = root.join("large.txt");
        std::fs::write(&path, "12345").expect("file should be written");
        let error = read_to_string_bounded(&path, Some(4)).expect_err("oversized read should fail");
        assert!(error.to_string().contains("max_bytes"));
        let hash_error = hash_resource_file(&path, Some(4))
            .expect_err("oversized files should stop hashing at the configured bound");
        assert!(hash_error.to_string().contains("max_bytes"));
        assert_eq!(
            read_to_string_bounded(&path, Some(5)).expect("bounded read should succeed"),
            "12345"
        );
        assert_eq!(
            hash_resource_file(&path, Some(5))
                .expect("bounded file hash should succeed")
                .1,
            5
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }
}
