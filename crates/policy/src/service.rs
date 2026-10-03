//! Service identity helper: human-readable log identifier.
//!
//! Returns the explicitly configured `SERVICE_NAME` when set, otherwise the
//! current binary file stem, otherwise a neutral fallback. Product names
//! never appear as literals; renaming the binary changes logs automatically.

/// Resolve the service identifier for logs and telemetry.
///
/// `SERVICE_NAME` wins when non-empty. Otherwise the current executable file
/// stem is used. Falls back to `server` when neither is available.
pub fn default_service_name() -> String {
    if let Ok(name) = std::env::var("SERVICE_NAME")
        && !name.trim().is_empty()
    {
        return name;
    }
    if let Some(stem) = binary_stem()
        && !stem.trim().is_empty()
    {
        return stem;
    }
    "server".to_string()
}

/// Current binary file stem without extension, if determinable.
///
/// Used for product-derived distribution paths (`share/<stem>/...`) so
/// renaming the binary changes bundled lookup automatically. Returns `None`
/// when the executable path is unknown instead of guessing a product name.
pub fn binary_stem() -> Option<String> {
    let exe = std::env::current_exe().ok()?;
    let stem = exe.file_stem()?.to_str()?;
    if stem.trim().is_empty() {
        return None;
    }
    Some(stem.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_name_resolution_order() {
        if env_process::isolated() {
            return;
        }
        // Single test owns SERVICE_NAME end to end: parallel tests in one
        // process share the environment, so split tests racing on the same
        // variable would flake. Follows the limits.rs set/restore pattern.
        // SAFETY: no other test in this process touches SERVICE_NAME.
        unsafe {
            std::env::set_var("SERVICE_NAME", "custom-service");
        }
        assert_eq!(default_service_name(), "custom-service");
        unsafe {
            std::env::set_var("SERVICE_NAME", "   ");
        }
        let fallback = default_service_name();
        unsafe {
            std::env::remove_var("SERVICE_NAME");
        }
        // Under cargo test the executable stem is the test binary name,
        // never empty; the fallback path stays covered by construction.
        assert!(!fallback.trim().is_empty(), "fallback must be non-blank");
    }

    #[test]
    fn binary_stem_matches_current_executable() {
        let expected = std::env::current_exe()
            .expect("test must know its executable")
            .file_stem()
            .expect("executable must have a stem")
            .to_str()
            .expect("stem must be UTF-8")
            .to_string();
        assert_eq!(binary_stem(), Some(expected));
    }
}

#[cfg(test)]
mod env_process {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/test-support/environment.rs"
    ));
}
