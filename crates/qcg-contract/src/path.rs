use camino::{Utf8Path, Utf8PathBuf};
use qcg_types::is_safe_relative_path;
use std::path::PathBuf;

/// Errors returned while resolving a path declared by a generator package.
#[derive(Debug, thiserror::Error)]
pub enum PackagePathError {
    #[error("package path `{path}` is not a safe relative path")]
    Unsafe { path: String },
    #[error("package root `{root}` could not be canonicalized: {source}")]
    Root {
        root: Utf8PathBuf,
        source: std::io::Error,
    },
    #[error("package path `{path}` could not be canonicalized: {source}")]
    Path {
        path: Utf8PathBuf,
        source: std::io::Error,
    },
    #[error("package path `{path}` escapes package root `{root}`")]
    Escapes { path: String, root: Utf8PathBuf },
    #[error("package path `{path}` is not valid UTF-8")]
    NonUtf8 { path: PathBuf },
}

/// Resolve an existing package-relative path to its canonical location.
///
/// Package paths are persisted in manifests and are therefore deliberately
/// platform-independent. The candidate and package root are canonicalized
/// before the component-aware containment check, so symlinks cannot escape the
/// package even when the textual path itself is safe.
pub fn resolve_package_path(
    root: impl AsRef<Utf8Path>,
    relative: &str,
) -> Result<Utf8PathBuf, PackagePathError> {
    if !is_safe_relative_path(relative) {
        return Err(PackagePathError::Unsafe {
            path: relative.to_string(),
        });
    }

    let root = root.as_ref();
    let canonical_root = std::fs::canonicalize(root).map_err(|source| PackagePathError::Root {
        root: root.to_path_buf(),
        source,
    })?;
    let candidate = root.join(relative);
    let canonical_candidate =
        std::fs::canonicalize(&candidate).map_err(|source| PackagePathError::Path {
            path: candidate,
            source,
        })?;
    if !canonical_candidate.starts_with(&canonical_root) {
        return Err(PackagePathError::Escapes {
            path: relative.to_string(),
            root: Utf8PathBuf::from_path_buf(canonical_root).unwrap_or_else(|_| root.to_path_buf()),
        });
    }
    Utf8PathBuf::from_path_buf(canonical_candidate)
        .map_err(|path| PackagePathError::NonUtf8 { path })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> Utf8PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-package-path-{name}-{}-{suffix}",
            std::process::id()
        )))
        .expect("temporary path should be UTF-8")
    }

    #[test]
    fn resolves_existing_relative_paths_to_canonical_locations() {
        let root = test_root("canonical");
        std::fs::create_dir_all(root.join("nested")).expect("package root should be created");
        std::fs::write(root.join("nested/file.txt"), "content").expect("file should be written");

        let resolved = resolve_package_path(&root, "nested/file.txt")
            .expect("safe package path should resolve");
        assert_eq!(
            resolved,
            Utf8PathBuf::from_path_buf(
                std::fs::canonicalize(root.join("nested/file.txt")).unwrap()
            )
            .unwrap()
        );

        std::fs::remove_dir_all(root).expect("temporary package should be removed");
    }

    #[test]
    fn rejects_unsafe_relative_paths_before_touching_the_filesystem() {
        let root = test_root("unsafe");
        std::fs::create_dir_all(&root).expect("package root should be created");
        for path in [
            "",
            "../outside",
            "nested/../file",
            "/absolute",
            "nested\\file",
        ] {
            assert!(matches!(
                resolve_package_path(&root, path),
                Err(PackagePathError::Unsafe { .. })
            ));
        }
        std::fs::remove_dir_all(root).expect("temporary package should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape_even_when_the_textual_path_is_relative() {
        use std::os::unix::fs::symlink;

        let root = test_root("symlink");
        let outside = test_root("outside");
        std::fs::create_dir_all(&root).expect("package root should be created");
        std::fs::create_dir_all(&outside).expect("outside directory should be created");
        std::fs::write(outside.join("secret.txt"), "secret")
            .expect("outside file should be written");
        symlink(&outside, root.join("link")).expect("symlink should be created");

        let error = resolve_package_path(&root, "link/secret.txt")
            .expect_err("symlink escape must be rejected");
        assert!(matches!(error, PackagePathError::Escapes { .. }), "{error}");

        std::fs::remove_dir_all(root).expect("temporary package should be removed");
        std::fs::remove_dir_all(outside).expect("temporary outside directory should be removed");
    }
}
