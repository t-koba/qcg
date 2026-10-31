//! Path syntax and collision checks are host-independent; filesystem length limits are not. Set keys use conservative NFD/full Unicode case-fold/NFD.
//! Safe paths are nonempty relative slash paths excluding ADS/drive colons, backslashes, controls, trailing dots/spaces, reserved stems and Win32 `< > " | ? *`; globs also allow `*` and `?`.
use camino::Utf8Path;

/// Canonical caseless key: NFD/full Unicode case-fold/NFD; conservative, not exact NTFS/APFS emulation.
pub fn path_collision_key(path: &str) -> String {
    use unicode_normalization::UnicodeNormalization as _;
    let nfd = path.nfd().collect::<String>();
    let folded = unicase::UniCase::unicode(nfd).to_folded_case();
    folded.nfd().collect()
}

/// Validates concrete path entries, preserving spelling and rejecting equivalent names or file parents.
pub fn validate_path_set<'a>(
    entries: impl IntoIterator<Item = (&'a str, bool)>,
) -> std::io::Result<()> {
    validate_path_set_inner(entries, false)
}

/// Validates path trees by components, preserving one spelling per directory including implicit parents.
pub fn validate_path_tree_set<'a>(
    entries: impl IntoIterator<Item = (&'a str, bool)>,
) -> std::io::Result<()> {
    validate_path_set_inner(entries, true)
}

fn validate_path_set_inner<'a>(
    entries: impl IntoIterator<Item = (&'a str, bool)>,
    tree: bool,
) -> std::io::Result<()> {
    let mut paths = entries
        .into_iter()
        .map(|(p, d)| (path_collision_key(p), p, d))
        .collect::<Vec<_>>();
    if paths.iter().any(|(_, p, _)| !is_safe_relative_path(p)) {
        return Err(std::io::Error::other("unsafe or colliding path set"));
    }
    paths.sort_unstable_by(|a, b| a.0.split('/').cmp(b.0.split('/')));
    for pair in paths.windows(2) {
        let (lk, lp, _) = &pair[0];
        let (rk, rp, _) = &pair[1];
        let alias = tree
            && lk
                .split('/')
                .zip(lp.split('/'))
                .zip(rk.split('/').zip(rp.split('/')))
                .take_while(|((left, _), (right, _))| left == right)
                .any(|((_, left), (_, right))| left != right);
        if lk == rk || alias {
            return Err(std::io::Error::other("unsafe or colliding path set"));
        }
    }
    for (key, _, _) in &paths {
        let mut parent = key.as_str();
        while let Some((p, _)) = parent.rsplit_once('/') {
            if let Ok(index) = paths.binary_search_by(|e| e.0.split('/').cmp(p.split('/')))
                && !paths[index].2
            {
                return Err(std::io::Error::other("path set uses a file as a directory"));
            }
            parent = p;
        }
    }
    Ok(())
}

const WINDOWS_RESERVED_STEMS: [&str; 30] = [
    "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "COM1", "COM2", "COM3", "COM4", "COM5",
    "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8",
    "LPT9", "COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³",
];

fn is_portable_component(part: &str, wildcards_allowed: bool) -> bool {
    if part.is_empty()
        || part == "."
        || part == ".."
        || part.contains(':')
        || part.ends_with(['.', ' '])
        || part.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || part.contains(['<', '>', '"', '|'])
        || (!wildcards_allowed && part.contains(['?', '*']))
    {
        return false;
    }
    let stem = part.split('.').next().unwrap_or(part);
    !WINDOWS_RESERVED_STEMS
        .iter()
        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
}

fn is_safe_relative(path: &str, w: bool) -> bool {
    !path.contains('\\') && path.split('/').all(|p| is_portable_component(p, w))
}

/// Returns whether `path` is a portable concrete relative path.
pub fn is_safe_relative_path(path: &str) -> bool {
    is_safe_relative(path, false)
}

/// Returns whether `pattern` is a portable relative glob (see module rules).
pub fn is_safe_glob_pattern(pattern: &str) -> bool {
    is_safe_relative(pattern, true)
}

pub fn is_safe_path_component(path: &str) -> bool {
    is_safe_relative_path(path) && !path.contains('/')
}

/// Joins path components with `/` for archive entries and manifests.
pub fn portable_relative_path(path: &Utf8Path) -> String {
    path.components()
        .map(|c| c.as_str())
        .collect::<Vec<_>>()
        .join("/")
}

/// Normalizes a safe fs `path_prefix` by trimming one trailing slash.
pub fn normalize_path_prefix(prefix: &str) -> Option<&str> {
    let normalized = prefix.strip_suffix('/').unwrap_or(prefix);
    is_safe_relative_path(normalized).then_some(normalized)
}

/// Tests safe component-boundary containment; equality counts, while unsafe or split-prefix paths fail.
pub fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    let Some(prefix) = normalize_path_prefix(prefix) else {
        return false;
    };
    let suffix = path.strip_prefix(prefix);
    is_safe_relative_path(path) && suffix.is_some_and(|s| s.is_empty() || s.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_trees_reject_aliasing_implicit_parents() {
        for entries in [
            vec![("A/x", false), ("a/y", false)],
            vec![("A", true), ("a/x", false)],
            vec![("A/B/x", false), ("a/C/y", false)],
            vec![("é/x", false), ("e\u{301}/y", false)],
            vec![("Maße/x", false), ("MASSE/y", false)],
        ] {
            assert!(validate_path_tree_set(entries).is_err());
        }
        for entries in [
            vec![("A/x", false), ("A/y", false)],
            vec![("A", true), ("A/x", false), ("A/y", false)],
            vec![("A/B/x", false), ("A/C/y", false)],
        ] {
            validate_path_tree_set(entries).unwrap();
        }
        assert!(validate_path_set([("A/x", false), ("a/y", false)]).is_ok());
    }

    #[test]
    fn path_trees_reject_nonadjacent_explicit_parent_aliases() {
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for entries in [
            [("A", true), ("a-b", false), ("a/x", false)],
            [("A/B", true), ("a/b-", false), ("a/b/x", false)],
        ] {
            for order in orders {
                let entries = order.map(|index| entries[index]);
                assert!(validate_path_tree_set(entries).is_err());
            }
        }
        validate_path_tree_set([("A", true), ("A/x", false), ("A-b", false)]).unwrap();
        validate_path_tree_set([("A/B", true), ("A/B/x", false), ("A/B-", false)]).unwrap();
    }

    #[test]
    fn path_sets_use_canonical_full_fold_and_component_prefixes() {
        for (a, b) in [
            ("Report.md", "report.md"),
            ("Maße", "MASSE"),
            ("é.txt", "e\u{301}.txt"),
            ("A/ß", "a/SS"),
            ("α\u{345}\u{301}", "α\u{301}\u{345}"),
        ] {
            assert_eq!(path_collision_key(a), path_collision_key(b), "{a} {b}");
            assert!(validate_path_set([(a, false), (b, false)]).is_err());
            assert!(validate_path_set([(b, true), (a, false)]).is_err());
            assert!(validate_path_set([(a, true), (b, true)]).is_err());
        }
        for entries in [
            vec![("A", false), ("a/x", false)],
            vec![("a/x", true), ("A", false)],
            vec![("Maße", false), ("MASSE/x", false)],
            vec![("é", false), ("e\u{301}/x", true)],
            vec![("a", false), ("a-b", false), ("a/b", false)],
        ] {
            assert!(validate_path_set(entries).is_err());
        }
        for entries in [
            vec![],
            vec![("A/x", false), ("a/y", false)],
            vec![("A", true), ("a/x", false), ("A/y", false)],
            vec![("a", false), ("ab/x", false)],
            vec![("é/x", false), ("e\u{301}/y", false)],
            vec![("e", false), ("é", false)],
            vec![("Ａ", false), ("A", false)],
        ] {
            validate_path_set(entries).unwrap();
        }
        assert!(validate_path_set([("same", false), ("same", false)]).is_err());
        assert!(validate_path_set([("same", true), ("same", true)]).is_err());
        assert!(validate_path_set([("NUL", false)]).is_err());
    }

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
