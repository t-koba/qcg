//! Platform-independent path safety predicates. Paths are persisted in
//! contracts and journals and may be consumed on another host, so these
//! checks never depend on the local filesystem.

use camino::Utf8Path;

/// Returns whether `path` is a safe, non-empty, relative slash-separated path.
/// Device names Windows cannot represent as files. Matching is
/// case-insensitive against the stem before the first `.`, because
/// `NUL.txt` is the `NUL` device on Windows.
const WINDOWS_RESERVED_STEMS: [&str; 24] = [
    "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "COM1", "COM2", "COM3", "COM4", "COM5",
    "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8",
    "LPT9",
];

fn equals_ignore_ascii_case(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .all(|(left, right)| left.to_ascii_uppercase() == right)
}

/// Returns whether a single `/`-separated component survives Win32
/// normalization unchanged: no ADS/drive colon, no stripped trailing
/// dot/space, no reserved device stem, no control bytes.
fn is_portable_component(part: &str) -> bool {
    if part.is_empty() || part == "." || part == ".." {
        return false;
    }
    if part.contains(':') {
        return false;
    }
    if part.ends_with('.') || part.ends_with(' ') {
        return false;
    }
    if part.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
        return false;
    }
    let stem = part.split('.').next().unwrap_or(part);
    if WINDOWS_RESERVED_STEMS
        .iter()
        .any(|reserved| equals_ignore_ascii_case(stem, reserved))
    {
        return false;
    }
    true
}

pub fn is_safe_relative_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    !path.is_empty()
        && !path.starts_with('/')
        && !(bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
        && !path.contains('\\')
        && !path.contains('\0')
        && !path.split('/').any(|part| !is_portable_component(part))
}

pub fn is_safe_path_component(path: &str) -> bool {
    is_safe_relative_path(path) && !path.contains('/')
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

    #[test]
    fn rejects_windows_portable_forms() {
        // ADS streams and drive-relative colons: distinct names that
        // redirect to one stream or device on a Windows host.
        // Reserved devices, with and without extensions (`NUL.txt`
        // is the `NUL` device), case-insensitively.
        // Trailing dots/spaces that Win32 strips, collapsing
        // distinct contract names onto one file.
        for bad in [
            "a/file.txt:secret",
            "a/file.txt:$DATA",
            "a/file:evil",
            "a/C:x",
            "a/CON",
            "a/CON.txt",
            "a/con.log.gz",
            "a/NUL",
            "a/nul.txt",
            "a/COM1",
            "a/com1.log",
            "a/LPT1.txt",
            "a/AUX",
            "a/file.",
            "a/file. ",
            "a/dir./x",
            "a/bad\u{7}name",
            "CON",
            "nul.txt",
        ] {
            assert!(!is_safe_relative_path(bad), "{bad:?}");
            if !bad.contains('/') {
                assert!(!is_safe_path_component(bad), "{bad:?}");
            }
        }
        for good in ["a/normal.txt", "a.b", "console.log", "auxiliary/data.txt"] {
            assert!(is_safe_relative_path(good), "{good:?}");
        }
    }
}
