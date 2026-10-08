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

/// Normalizes an fs tool `path_prefix` to its slash-trimmed safe form.
/// Returns `None` when the prefix is not a safe relative path.
pub fn normalize_path_prefix(prefix: &str) -> Option<&str> {
    let normalized = prefix.strip_suffix('/').unwrap_or(prefix);
    is_safe_relative_path(normalized).then_some(normalized)
}

/// Returns whether `path` stays within `prefix` on component boundaries.
/// Both sides must be safe relative paths; `path == prefix` counts as
/// within. String-prefix matches that split a component (e.g.
/// `outcome.txt` under `out`) are rejected, as is any unsafe path.
pub fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    let Some(prefix) = normalize_path_prefix(prefix) else {
        return false;
    };
    is_safe_relative_path(path)
        && (path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/')))
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
    fn prefix_matches_components_not_string_prefixes() {
        assert!(path_is_within_prefix("out/result.txt", "out/"));
        assert!(path_is_within_prefix("out/result.txt", "out"));
        assert!(path_is_within_prefix("out", "out/"));
        assert!(!path_is_within_prefix("outcome.txt", "out/"));
        assert!(!path_is_within_prefix("outcome/result.txt", "out"));
        assert!(!path_is_within_prefix("other/result.txt", "out/"));
        assert!(!path_is_within_prefix("out/../escape.txt", "out/"));
        assert!(normalize_path_prefix("out/") == Some("out"));
        assert!(normalize_path_prefix("../escape").is_none());
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
