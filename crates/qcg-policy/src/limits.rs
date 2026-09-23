//! Canonical numeric policy bounds shared by every layer.
//!
//! Each bound lives here exactly once. Crates must reference these items
//! instead of redeclaring values locally, so policy changes stay consistent
//! and reviewable in one place. `None`-style opt-outs stay with the caller:
//! these constants only fix the numeric policy, never its interpretation.

use std::time::Duration;

/// Default maximum directory entries scanned when listing run or
/// generator roots. The mechanism bound is `MAX_DIRECTORY_SCAN_ENTRIES`;
/// deployments may set any value within the bounds.
pub const DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES: usize = 100_000;

/// Inclusive mechanism bound for the directory scan cap.
pub const MAX_DIRECTORY_SCAN_ENTRIES: usize = 10_000_000;
pub const MIN_DIRECTORY_SCAN_ENTRIES: usize = 1_000;

/// Inclusive mechanism bounds for the per-run live broadcast channel.
pub const MIN_LIVE_EVENT_CHANNEL_CAPACITY: usize = 16;
pub const MAX_LIVE_EVENT_CHANNEL_CAPACITY: usize = 65_536;

/// Inclusive mechanism bounds for the shared-mode journal poll cadence.
pub const MIN_JOURNAL_POLL_INTERVAL_MILLIS: u64 = 50;
pub const MAX_JOURNAL_POLL_INTERVAL_MILLIS: u64 = 5_000;

/// Inclusive mechanism bounds for shared-store rescan and the queued
/// resumer cadence, in seconds.
pub const MIN_STORE_RESCAN_SECS: u64 = 1;
pub const MAX_STORE_RESCAN_SECS: u64 = 3_600;

/// Default in-memory scan window for journal tail repair and seq
/// recovery. The mechanism bounds keep repair memory flat.
pub const DEFAULT_JOURNAL_SCAN_WINDOW_BYTES: usize = 1024 * 1024;
pub const MIN_JOURNAL_SCAN_WINDOW_BYTES: usize = 4_096;
pub const MAX_JOURNAL_SCAN_WINDOW_BYTES: usize = 64 * 1024 * 1024;

/// Inclusive mechanism bounds for the graceful shutdown phases, in
/// seconds. The settle deadline must be at least the drain timeout.
pub const MIN_SHUTDOWN_PHASE_SECS: u64 = 1;
pub const MAX_SHUTDOWN_PHASE_SECS: u64 = 3_600;

/// Maximum bytes read from a credential or secret file.
pub const MAX_CREDENTIAL_FILE_BYTES: u64 = 64 * 1024;

/// Inclusive upper bound for `foreach` iteration counts.
pub const MAX_FOREACH_ITERATIONS: usize = 10_000;

/// Default number of terminal runs kept by the automatic retention sweep.
pub const DEFAULT_GC_KEEP: usize = 50;

/// Default additional failed runs kept for post-mortems.
pub const DEFAULT_GC_KEEP_FAILED: usize = 10;

/// Default seconds between automatic retention sweeps (24 hours).
pub const DEFAULT_GC_INTERVAL_SECS: u64 = 24 * 60 * 60;

/// Maximum free-form run metadata labels accepted per run.
pub const MAX_RUN_LABELS: usize = 32;

/// Maximum bytes for one run label key or value.
pub const MAX_RUN_LABEL_BYTES: usize = 256;

/// Inclusive upper bound for LLM agent tool iterations per call.
pub const MAX_AGENT_ITERATIONS: usize = 32;

/// Inclusive upper bound for declared max tool calls on one tool.
pub const MAX_AGENT_TOOL_CALLS: usize = 10;

/// Inclusive upper bound for `web.search` results per call.
pub const MAX_WEB_SEARCH_RESULTS: usize = 20;

/// Inclusive upper bound for declared fallback models on one tool.
pub const MAX_FALLBACK_MODELS: usize = 8;

/// Inclusive upper bound for retry attempts on one node or hook.
pub const MAX_RETRY_ATTEMPTS: u32 = 16;

/// Inclusive upper bound for fixed retry backoff, in milliseconds.
pub const MAX_RETRY_BACKOFF_MS: u64 = 60_000;

/// Inclusive upper bound for `foreach` parallel fan-out.
pub const MAX_FOREACH_PARALLELISM: usize = 256;

/// Default cap for total executed steps per run.
pub const DEFAULT_MAX_TOTAL_STEPS: usize = 10_000;

/// Default cap for concurrently executing runs per service.
pub const DEFAULT_MAX_ACTIVE_RUNS: usize = 8;

/// Default cap for tracked runs retained per service.
pub const DEFAULT_MAX_TRACKED_RUNS: usize = 4_096;

/// Capacity of the journal polling channel per run.
pub const JOURNAL_POLL_CHANNEL_CAPACITY: usize = 128;

/// Poll interval for shared-mode journal followers. Deliberately fixed:
/// every subscriber observes identical progress on the same cadence, and
/// the lagged-resync contract (reconnect from the last delivered seq)
/// assumes a bounded, uniform poll rather than a tunable one (E12).
pub const JOURNAL_POLL_INTERVAL_MILLIS: u64 = 250;

/// Capacity of the per-run live broadcast channel. A lagged receiver gets
/// a `lagged` marker with its last delivered seq instead of a fabricated
/// cursor, so sizing affects responsiveness, not correctness (E12).
pub const LIVE_EVENT_CHANNEL_CAPACITY: usize = 512;

/// Maximum bytes accepted for a package signing key file.
pub const MAX_SIGNING_KEY_BYTES: usize = 64 * 1024;

/// Maximum bytes accepted for one interactive confirmation answer.
pub const MAX_CONFIRM_INPUT_BYTES: usize = 1024;

/// Number of synthesized entries in a generated package archive.
pub const GENERATED_PACKAGE_ENTRIES: usize = 2;

/// Maximum bytes accepted for one interactive step input line.
pub const MAX_INTERACTIVE_INPUT_BYTES: usize = 64 * 1024;

/// Maximum result files collected from a single command execution.
pub const MAX_COMMAND_RESULT_FILES: usize = 1024;

/// Default timeout, in seconds, for MCP server operations.
pub const DEFAULT_MCP_TIMEOUT_SECONDS: u64 = 120;

/// Default cap, in bytes, for MCP responses.
pub const DEFAULT_MCP_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Request header carrying the idempotency key.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";

/// How long an idempotency record is retained.
pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum idempotency records retained per server.
pub const IDEMPOTENCY_MAX_ENTRIES: usize = 1024;

/// Maximum bytes kept per tool-call event value before truncation.
pub const TOOL_EVENT_VALUE_LIMIT_BYTES: usize = 32 * 1024;

/// Default cap, in bytes, for LLM request context.
pub const DEFAULT_LLM_CONTEXT_LIMIT_BYTES: usize = 1024 * 1024;

/// Parses a boolean deployment knob with exactly one standard: `1`, `true`,
/// `yes`, or `on` (any case) enable; `0`, `false`, `no`, or `off` disable;
/// unset means `default`. Anything else — including the empty string — is
/// an error naming the variable, never a silent truthy/falsy fold (E04).
pub fn parse_bool_env(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!(
                "invalid {name} `{value}`: must be one of 1/true/yes/on or 0/false/no/off"
            )),
        },
    }
}

#[cfg(test)]
mod bool_env_tests {
    use super::parse_bool_env;

    #[test]
    fn bool_knobs_reject_garbage_instead_of_folding() {
        for (value, expected) in [
            ("1", true),
            ("true", true),
            ("YES", true),
            ("on", true),
            ("0", false),
            ("false", false),
            ("NO", false),
            ("off", false),
        ] {
            // SAFETY: test-only serial env mutation would need a lock;
            // this suite sets one variable and restores it immediately.
            unsafe {
                std::env::set_var("QCG_POLICY_BOOL_TEST", value);
            }
            assert_eq!(
                parse_bool_env("QCG_POLICY_BOOL_TEST", true),
                Ok(expected),
                "value `{value}` must parse"
            );
            unsafe {
                std::env::remove_var("QCG_POLICY_BOOL_TEST");
            }
        }
        for garbage in ["2", "yes please", "", "enabled", "tru"] {
            unsafe {
                std::env::set_var("QCG_POLICY_BOOL_TEST", garbage);
            }
            let error =
                parse_bool_env("QCG_POLICY_BOOL_TEST", true).expect_err("garbage must fail");
            assert!(
                error.contains("QCG_POLICY_BOOL_TEST"),
                "the error must name the variable: {error}"
            );
            unsafe {
                std::env::remove_var("QCG_POLICY_BOOL_TEST");
            }
        }
        unsafe {
            std::env::remove_var("QCG_POLICY_BOOL_TEST");
        }
        assert_eq!(parse_bool_env("QCG_POLICY_BOOL_TEST", true), Ok(true));
        assert_eq!(parse_bool_env("QCG_POLICY_BOOL_TEST", false), Ok(false));
    }
}
