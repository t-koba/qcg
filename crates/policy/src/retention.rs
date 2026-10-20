//! Deployment retention policy: the counts a sweep keeps and the store scan
//! bound it applies.
//!
//! The budget is resolved once per deployment so every sweep path — the
//! server task and the command-line paths — applies the same counts. The
//! expiry rule itself belongs to the run journal, not here: a policy that
//! says how many runs survive never decides when a run expires.

use crate::limits::{
    DEFAULT_GC_KEEP, DEFAULT_GC_KEEP_FAILED, DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
    MAX_DIRECTORY_SCAN_ENTRIES, MIN_DIRECTORY_SCAN_ENTRIES, parse_bounded_env, parse_positive_env,
};

/// Budget one retention sweep applies, resolved once from the environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Newest nonfailed terminal runs a sweep keeps.
    pub keep: usize,
    /// Failed runs a sweep keeps for post-mortems, separate from `keep`.
    pub keep_failed: usize,
    /// Store entries one sweep may scan before it refuses the pass.
    pub max_scan_entries: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep: DEFAULT_GC_KEEP,
            keep_failed: DEFAULT_GC_KEEP_FAILED,
            max_scan_entries: DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        }
    }
}

impl RetentionPolicy {
    /// Resolve the retention budget from the deployment environment.
    ///
    /// Errors name the offending variable and carry no prefix of their own:
    /// each caller adds the context its own surface needs.
    pub fn from_env() -> Result<Self, String> {
        let keep = parse_positive_env("GC_KEEP", DEFAULT_GC_KEEP)?;
        let keep_failed = parse_positive_env("GC_KEEP_FAILED", DEFAULT_GC_KEEP_FAILED)?;
        let max_scan_entries = usize::try_from(parse_bounded_env(
            "MAX_DIRECTORY_SCAN_ENTRIES",
            DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES as u64,
            MIN_DIRECTORY_SCAN_ENTRIES as u64,
            MAX_DIRECTORY_SCAN_ENTRIES as u64,
        )?)
        .map_err(|_| "MAX_DIRECTORY_SCAN_ENTRIES is not representable".to_string())?;
        Ok(Self {
            keep,
            keep_failed,
            max_scan_entries,
        })
    }

    /// Apply explicit operator counts over the resolved budget.
    ///
    /// A `None` leaves the resolved value in place, so an explicit count
    /// overrides the environment and never silently replaces it with a
    /// default.
    pub fn with_counts(mut self, keep: Option<usize>, keep_failed: Option<usize>) -> Self {
        if let Some(keep) = keep {
            self.keep = keep;
        }
        if let Some(keep_failed) = keep_failed {
            self.keep_failed = keep_failed;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::env_process;
    use super::*;

    fn set(name: &str, value: &str) {
        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::set_var(name, value) };
    }

    fn unset(name: &str) {
        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::remove_var(name) };
    }

    #[test]
    fn retention_budget_resolves_from_the_environment() {
        if env_process::isolated() {
            return;
        }
        unset("GC_KEEP");
        unset("GC_KEEP_FAILED");
        unset("MAX_DIRECTORY_SCAN_ENTRIES");
        assert_eq!(
            RetentionPolicy::from_env().expect("defaults should resolve"),
            RetentionPolicy::default()
        );
        set("GC_KEEP", "5");
        set("GC_KEEP_FAILED", "2");
        set("MAX_DIRECTORY_SCAN_ENTRIES", "2000");
        let resolved = RetentionPolicy::from_env().expect("explicit counts should resolve");
        assert_eq!(resolved.keep, 5);
        assert_eq!(resolved.keep_failed, 2);
        assert_eq!(resolved.max_scan_entries, 2000);
    }

    #[test]
    fn explicit_counts_override_only_the_counts_they_name() {
        let policy = RetentionPolicy::default().with_counts(Some(3), None);
        assert_eq!(policy.keep, 3);
        assert_eq!(policy.keep_failed, DEFAULT_GC_KEEP_FAILED);
        assert_eq!(policy.max_scan_entries, DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES);
    }

    #[test]
    fn invalid_retention_counts_refuse_naming_the_variable() {
        if env_process::isolated() {
            return;
        }
        set("GC_KEEP", "0");
        let error = RetentionPolicy::from_env().expect_err("a zero keep count must refuse");
        assert!(
            error.contains("GC_KEEP"),
            "the error must name the variable: {error}"
        );
        unset("GC_KEEP");
        set("MAX_DIRECTORY_SCAN_ENTRIES", "10");
        let error =
            RetentionPolicy::from_env().expect_err("an out-of-range scan bound must refuse");
        assert!(
            error.contains("MAX_DIRECTORY_SCAN_ENTRIES"),
            "the error must name the variable: {error}"
        );
        unset("MAX_DIRECTORY_SCAN_ENTRIES");
    }
}

#[cfg(test)]
mod env_process {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/test-support/environment.rs"
    ));
}
