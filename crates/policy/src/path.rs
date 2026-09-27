//! Platform-independent path safety predicates. Paths are persisted in
//! contracts and journals and may be consumed on another host, so these
//! checks never depend on the local filesystem.

use camino::Utf8Path;

/// Returns whether `path` is a safe, non-empty, relative slash-separated path.
pub fn is_safe_relative_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    !path.is_empty()
        && !path.starts_with('/')
        && !(bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
        && !path.contains('\\')
        && !path.contains('\0')
        && !path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
}

pub fn is_safe_path_component(path: &str) -> bool {
    is_safe_relative_path(path)
        && !path.contains('/')
        && !path.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
}

/// Joins path components with `/` for archive entries and manifests.
pub fn portable_relative_path(path: &Utf8Path) -> String {
    path.components()
        .map(|component| component.as_str())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal_and_absolute_forms() {
        for bad in [
            "",
            "/",
            "/etc/passwd",
            "a/../b",
            "a/./b",
            "a//b",
            "..",
            ".",
            "C:/x",
            "a\\b",
            "a\0b",
        ] {
            assert!(!is_safe_relative_path(bad), "{bad:?}");
        }
        for good in ["a", "a/b/c", "file-name_1.json", "nested/dir/"] {
            if good.ends_with('/') {
                assert!(!is_safe_relative_path(good), "{good:?}");
                continue;
            }
            assert!(is_safe_relative_path(good), "{good:?}");
        }
    }

    #[test]
    fn rejects_unsafe_single_components() {
        for bad in ["", ".", "..", "a/b", "a\\b", "a\0b", "a\nb", "a\x7fb"] {
            assert!(!is_safe_path_component(bad), "{bad:?}");
        }
        for good in ["a", "generator-1", "run_2", "a.b"] {
            assert!(is_safe_path_component(good), "{good:?}");
        }
    }
}
