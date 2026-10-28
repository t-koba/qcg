//! Platform-independent path safety predicates. Paths are persisted in
//! contracts and journals and may be consumed on another host, so these
//! checks never depend on the local filesystem.
//!
//! A safe relative path is non-empty, relative, slash-separated, and names
//! no form a Windows host cannot represent: no ADS or drive colon, no
//! backslash, no control byte, no trailing dot or space, no reserved device
//! stem, and none of the characters Win32 forbids in a file name (`<`, `>`,
//! `"`, `|`, `?`, `*`). A glob pattern is the same shape, except that a
//! component may hold the `*` and `?` wildcards.

use camino::Utf8Path;

/// Device names Windows cannot represent as files. Matching is
/// case-insensitive against the stem before the first `.`, because
/// `NUL.txt` is the `NUL` device on Windows.
const WINDOWS_RESERVED_STEMS: [&str; 30] = [
    "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "COM1", "COM2", "COM3", "COM4", "COM5",
    "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8",
    "LPT9", "COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³",
];

/// Characters Win32 forbids in a file name, apart from the colon and
/// the backslash that are refused separately. A path that names one
/// cannot exist on a Windows host, so a contract, journal or archive
/// entry carrying it is not portable. The `?` and `*` wildcards are
/// forbidden in a file name too, but only a glob pattern may hold them.
const WIN32_FORBIDDEN_CHARS: [char; 4] = ['<', '>', '"', '|'];

/// Validates one component against the module-level portable-path rules.
fn is_portable_component(part: &str, wildcards_allowed: bool) -> bool {
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
    if part.contains(WIN32_FORBIDDEN_CHARS) || (!wildcards_allowed && part.contains(['?', '*'])) {
        return false;
    }
    let stem = part.split('.').next().unwrap_or(part);
    if WINDOWS_RESERVED_STEMS
        .iter()
        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
    {
        return false;
    }
    true
}

fn is_safe_relative(path: &str, wildcards_allowed: bool) -> bool {
    let bytes = path.as_bytes();
    !path.is_empty()
        && !path.starts_with('/')
        && !(bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
        && !path.contains('\\')
        && !path.contains('\0')
        && !path
            .split('/')
            .any(|part| !is_portable_component(part, wildcards_allowed))
}

/// Returns whether `path` is a portable concrete relative path.
pub fn is_safe_relative_path(path: &str) -> bool {
    is_safe_relative(path, false)
}

/// Returns whether `pattern` is a portable relative glob.
/// The module documentation describes the wildcard and component rules.
pub fn is_safe_glob_pattern(pattern: &str) -> bool {
    is_safe_relative(pattern, true)
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
            // Characters Win32 forbids in a file name.
            "a/file<name",
            "a/file>name",
            "a/file\"name",
            "a/file|name",
            "a/file?name",
            "a/file*name",
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

    #[test]
    fn rejects_superscript_windows_device_stems() {
        for stem in [
            "COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³", "cOm¹", "lPt³",
        ] {
            for path in [
                stem.to_owned(),
                format!("{stem}.txt"),
                format!("a/{stem}.log.gz"),
            ] {
                assert!(!is_safe_relative_path(&path), "{path:?}");
                assert!(!is_safe_glob_pattern(&path), "{path:?}");
                if !path.contains('/') {
                    assert!(!is_safe_path_component(&path), "{path:?}");
                }
            }
        }
    }

    #[test]
    fn a_glob_pattern_may_hold_the_wildcards_only() {
        // A pattern is the only place the `*` and `?` wildcards may appear.
        for good in [
            "reports/*.json",
            "reports/**/*.json",
            "reports/?.json",
            "reports/report-*.json",
        ] {
            assert!(is_safe_glob_pattern(good), "{good:?}");
        }
        for bad in [
            "../escape",
            "/absolute",
            "a\\b",
            "a/bad<name",
            "a/bad>name",
            "a/bad\"name",
            "a/bad|name",
            "a/file:evil",
            "a/NUL",
        ] {
            assert!(!is_safe_glob_pattern(bad), "{bad:?}");
        }
    }
}
