//! Canonical numeric policy bounds shared by every layer.
//!
//! Each bound lives here exactly once. Crates must reference these items
//! instead of redeclaring values locally, so policy changes stay consistent
//! and reviewable in one place. `None`-style opt-outs stay with the caller:
//! these constants only fix the numeric policy, never its interpretation.

use serde::Deserialize;
use std::sync::OnceLock;
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

/// Bounds and defaults for the shared-store rescan and queued-resumer cadence (s).
pub const MIN_STORE_RESCAN_SECS: u64 = 1;
pub const MAX_STORE_RESCAN_SECS: u64 = 3_600;
pub const DEFAULT_STORE_RESCAN_SECS: u64 = 5;
pub const DEFAULT_QUEUED_RESUMER_SECS: u64 = 5;

pub const DEFAULT_JOURNAL_SCAN_WINDOW_BYTES: usize = 1024 * 1024;
pub const MIN_JOURNAL_SCAN_WINDOW_BYTES: usize = 4_096;
pub const MAX_JOURNAL_SCAN_WINDOW_BYTES: usize = 64 * 1024 * 1024;

pub const DEFAULT_MAX_JOURNAL_EVENT_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_JOURNAL_TOTAL_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_JOURNAL_EVENT_COUNT: usize = 1_000_000;
pub const DEFAULT_MAX_STATE_BYTES: usize = 16 * 1024 * 1024;

/// Retained service read-view charge, separately from in-flight parser buffers.
pub const DEFAULT_READ_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Default cap, in bytes, for the whole HTTP request body when
/// `--max-request-bytes` (`MAX_REQUEST_BYTES`) is omitted. Restores Axum's
/// 2 MiB secure default: omitted means this default, never unlimited, so
/// unauthenticated buffering and durable journal/idempotency growth stay
/// bounded before any contract or FileValue check.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

/// Default cap, in bytes, for one HTTP request or response body when the
/// contract's `runtime.http_body_limit_bytes` is omitted. The body is read
/// into memory, so an omitted limit must not mean an unbounded read: the
/// default bounds both directions of one HTTP exchange.
pub const DEFAULT_HTTP_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// Default combined stdout and stderr capture per command.
pub const DEFAULT_COMMAND_OUTPUT_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// Hard cap for one run's observation stream when `[audit].max_bytes` is
/// unset. The writer degrades instead of exceeding it, and every reader
/// refuses a stream that already did, so an audit trail can never become the
/// unbounded allocation a durable journal is not allowed to be.
pub const DEFAULT_MAX_AUDIT_TOTAL_BYTES: usize = 64 * 1024 * 1024;

/// Hard cap for one served generator package asset when
/// `MAX_ASSET_BYTES` is unset. The asset is read into memory to be
/// returned, so "no operator limit" must not mean "no limit".
pub const DEFAULT_MAX_ASSET_BYTES: usize = 16 * 1024 * 1024;

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

/// Default ceiling for LLM agent tool iterations per call.
/// The effective ceiling is `OperationalLimits::agent_iterations`.
pub const MAX_AGENT_ITERATIONS: usize = 32;

/// Default ceiling for declared max tool calls on one tool.
/// The effective ceiling is `OperationalLimits::agent_tool_calls`.
pub const MAX_AGENT_TOOL_CALLS: usize = 10;

/// Default ceiling for `web.search` results per call.
/// The effective ceiling is `OperationalLimits::web_search_results`.
pub const MAX_WEB_SEARCH_RESULTS: usize = 20;

/// Default ceiling for declared fallback models on one tool.
/// The effective ceiling is `OperationalLimits::fallback_models`.
pub const MAX_FALLBACK_MODELS: usize = 8;

/// Default ceiling for retry attempts on one node or hook.
/// The effective ceiling is `OperationalLimits::retry_attempts`.
pub const MAX_RETRY_ATTEMPTS: u32 = 16;

/// Default ceiling for fixed retry backoff, in milliseconds.
/// The effective ceiling is `OperationalLimits::retry_backoff_ms`.
pub const MAX_RETRY_BACKOFF_MS: u64 = 60_000;

/// Default completion timeout for provider rows without `timeout_seconds`.
pub const DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS: u64 = 120;

/// Default timeout for external catalog fetches and model discovery.
pub const DEFAULT_CATALOG_FETCH_TIMEOUT_SECONDS: u64 = 30;

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

/// Inclusive mechanism bounds for hash-anchored patch edits per call.
/// The policy layer resolves a contract value within these bounds and
/// passes a plain number to the mechanism.
pub const MIN_PATCH_EDITS: usize = 1;
pub const MAX_PATCH_EDITS: usize = 128;

/// Inclusive mechanism bounds for hash-anchored patch payload bytes
/// (sum of replacement line bytes, excluding file content).
pub const MIN_PATCH_BYTES: usize = 1;
pub const MAX_PATCH_BYTES: usize = 256 * 1024;

/// Inclusive mechanism bound for anchored read windows.
/// The lower bound is structural (line numbers start at 1) and enforced at
/// each call site, so only the upper bound lives here as policy.
pub const MAX_ANCHORED_READ_LINES: usize = 2_000;

/// Default anchored patch budgets applied when the contract leaves
/// the corresponding runtime limit unset.
pub const DEFAULT_PATCH_EDITS: usize = 32;
pub const DEFAULT_PATCH_BYTES: usize = 64 * 1024;

/// Q01: default patch target ceiling, in bytes. Hash-anchored patch and
/// repair refuse targets larger than the effective ceiling at the commit-time
/// re-check, no matter how high the caller sets `output_file_limit_bytes` /
/// `output_total_limit_bytes`. The effective ceiling is
/// `OperationalLimits::patch_target_bytes`; callers asking for more fail fast
/// with the ceiling displayed instead of failing later at the re-check.
pub const MAX_PATCH_TARGET_BYTES: usize = 8 * 1024 * 1024;

/// Default timeout, in seconds, for MCP server operations.
pub const DEFAULT_MCP_TIMEOUT_SECONDS: u64 = 120;

/// Default cap, in bytes, for MCP responses.
pub const DEFAULT_MCP_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Deployment ceilings for the eleven operational limits shared by contract
/// validation and execution. This is policy, not mechanism: the configured
/// values are the effective ceilings themselves. Defaults preserve today's
/// values; a deployment may tighten or raise a ceiling in `providers.toml`.
/// Contracts may declare values up to the effective ceiling; values above it
/// are refused at validation and at runtime, never silently clamped. The
/// provider timeout is the fallback for rows omitting `timeout_seconds` (an
/// explicit row value wins); the catalog timeout applies to both network paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationalLimits {
    /// Maximum tool calls on one agent tool, including `web.search`.
    #[serde(default = "default_agent_tool_calls")]
    pub agent_tool_calls: usize,
    /// Maximum agent iterations per call.
    #[serde(default = "default_agent_iterations")]
    pub agent_iterations: usize,
    /// Maximum fallback models declared on one tool.
    #[serde(default = "default_fallback_models")]
    pub fallback_models: usize,
    /// Maximum `web.search` results per call.
    #[serde(default = "default_web_search_results")]
    pub web_search_results: usize,
    /// Maximum retry attempts on one node or hook.
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    /// Maximum fixed retry backoff, in milliseconds. Zero is allowed: it
    /// means no delay, and matches today's contract validation.
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    /// Default completion timeout, in seconds, for provider rows that omit
    /// `timeout_seconds`. A row's explicit value takes precedence.
    #[serde(default = "default_provider_request_timeout_seconds")]
    pub provider_request_timeout_seconds: u64,
    /// Timeout, in seconds, for external catalog fetches and per-provider
    /// model discovery. Both paths use the same resolved value.
    #[serde(default = "default_catalog_fetch_timeout_seconds")]
    pub catalog_fetch_timeout_seconds: u64,
    /// Effective hash-anchored patch target ceiling, in bytes (Q01).
    #[serde(default = "default_patch_target_bytes")]
    pub patch_target_bytes: usize,
    /// Deployment ceiling for `[audit].max_tool_value_bytes` on the general
    /// tool-call path. Defaults to today's 32 KiB value.
    #[serde(default = "default_audit_tool_value_bytes")]
    pub audit_tool_value_bytes: usize,
    /// Deployment ceiling for `[audit].max_tool_value_bytes` on direct
    /// `mcp.call` events. Defaults to today's 64 KiB shipped detail limit.
    #[serde(default = "default_direct_mcp_tool_value_bytes")]
    pub direct_mcp_tool_value_bytes: usize,
}

const fn default_agent_tool_calls() -> usize {
    MAX_AGENT_TOOL_CALLS
}
const fn default_agent_iterations() -> usize {
    MAX_AGENT_ITERATIONS
}
const fn default_fallback_models() -> usize {
    MAX_FALLBACK_MODELS
}
const fn default_web_search_results() -> usize {
    MAX_WEB_SEARCH_RESULTS
}
const fn default_retry_attempts() -> u32 {
    MAX_RETRY_ATTEMPTS
}
const fn default_retry_backoff_ms() -> u64 {
    MAX_RETRY_BACKOFF_MS
}
const fn default_provider_request_timeout_seconds() -> u64 {
    DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS
}
const fn default_catalog_fetch_timeout_seconds() -> u64 {
    DEFAULT_CATALOG_FETCH_TIMEOUT_SECONDS
}
const fn default_patch_target_bytes() -> usize {
    MAX_PATCH_TARGET_BYTES
}
const fn default_audit_tool_value_bytes() -> usize {
    TOOL_EVENT_VALUE_LIMIT_BYTES
}
const fn default_direct_mcp_tool_value_bytes() -> usize {
    DEFAULT_DIRECT_MCP_TOOL_VALUE_BYTES
}

impl Default for OperationalLimits {
    fn default() -> Self {
        Self {
            agent_tool_calls: default_agent_tool_calls(),
            agent_iterations: default_agent_iterations(),
            fallback_models: default_fallback_models(),
            web_search_results: default_web_search_results(),
            retry_attempts: default_retry_attempts(),
            retry_backoff_ms: default_retry_backoff_ms(),
            provider_request_timeout_seconds: default_provider_request_timeout_seconds(),
            catalog_fetch_timeout_seconds: default_catalog_fetch_timeout_seconds(),
            patch_target_bytes: default_patch_target_bytes(),
            audit_tool_value_bytes: default_audit_tool_value_bytes(),
            direct_mcp_tool_value_bytes: default_direct_mcp_tool_value_bytes(),
        }
    }
}

impl OperationalLimits {
    /// Refuse only intrinsic ill-formedness: every count, byte target and
    /// timeout must be nonzero (a zero timeout would mean no bound); any
    /// `retry_backoff_ms` is valid (zero means no delay). There is no legacy
    /// upper bound: the configured values are the effective ceilings, so a
    /// deployment may raise them above the defaults.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("agent_tool_calls", self.agent_tool_calls),
            ("agent_iterations", self.agent_iterations),
            ("fallback_models", self.fallback_models),
            ("web_search_results", self.web_search_results),
            ("retry_attempts", self.retry_attempts as usize),
            (
                "provider_request_timeout_seconds",
                self.provider_request_timeout_seconds as usize,
            ),
            (
                "catalog_fetch_timeout_seconds",
                self.catalog_fetch_timeout_seconds as usize,
            ),
            ("patch_target_bytes", self.patch_target_bytes),
            ("audit_tool_value_bytes", self.audit_tool_value_bytes),
            (
                "direct_mcp_tool_value_bytes",
                self.direct_mcp_tool_value_bytes,
            ),
        ] {
            if value == 0 {
                return Err(format!("{name} must be at least 1"));
            }
        }
        Ok(())
    }
}

/// Default deployment ceiling, used until a registry installs its own.
/// Preserves today's values; kept as a `const` so reads never freeze the
/// process-wide installation (see `operational_limits`).
const DEFAULT_DEPLOYMENT_LIMITS: OperationalLimits = OperationalLimits {
    agent_tool_calls: MAX_AGENT_TOOL_CALLS,
    agent_iterations: MAX_AGENT_ITERATIONS,
    fallback_models: MAX_FALLBACK_MODELS,
    web_search_results: MAX_WEB_SEARCH_RESULTS,
    retry_attempts: MAX_RETRY_ATTEMPTS,
    retry_backoff_ms: MAX_RETRY_BACKOFF_MS,
    provider_request_timeout_seconds: DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS,
    catalog_fetch_timeout_seconds: DEFAULT_CATALOG_FETCH_TIMEOUT_SECONDS,
    patch_target_bytes: MAX_PATCH_TARGET_BYTES,
    audit_tool_value_bytes: TOOL_EVENT_VALUE_LIMIT_BYTES,
    direct_mcp_tool_value_bytes: DEFAULT_DIRECT_MCP_TOOL_VALUE_BYTES,
};

static DEPLOYMENT_LIMITS: OnceLock<OperationalLimits> = OnceLock::new();

/// Installs the deployment ceiling once, before contracts are validated.
/// Repeated installation of the same value is a no-op (a registry may be
/// parsed more than once, and CLI preload plus router install observe the
/// same file); a conflicting value is refused instead of changing policy
/// mid-process. Reads via `operational_limits` never install, so validation
/// before installation observes defaults without freezing a later install.
pub fn set_deployment_operational_limits(limits: OperationalLimits) -> Result<(), String> {
    limits.validate()?;
    match DEPLOYMENT_LIMITS.set(limits) {
        Ok(()) => Ok(()),
        Err(_) if DEPLOYMENT_LIMITS.get() == Some(&limits) => Ok(()),
        Err(_) => Err("operational limits are already frozen for this process".into()),
    }
}

/// Effective deployment ceiling. Unset means today's defaults; never an
/// unrestricted value. Every operational check reads through this accessor.
/// A read alone never installs: only `set_deployment_operational_limits`
/// freezes the process value, so tests and boot paths that validate before
/// installing still observe defaults without blocking a later install.
pub fn operational_limits() -> &'static OperationalLimits {
    DEPLOYMENT_LIMITS
        .get()
        .unwrap_or(&DEFAULT_DEPLOYMENT_LIMITS)
}

/// Effective completion timeout for one provider row: its explicit
/// `timeout_seconds`, else the deployment default. Callers pass the resolved
/// limits so the precedence is testable without freezing policy.
pub fn effective_provider_timeout_seconds(
    configured: Option<u64>,
    limits: &OperationalLimits,
) -> u64 {
    configured.unwrap_or(limits.provider_request_timeout_seconds)
}

/// Effective per-request timeout for external catalog fetches and provider
/// model discovery. Both network paths share this one resolved value.
pub fn catalog_fetch_timeout(limits: &OperationalLimits) -> Duration {
    Duration::from_secs(limits.catalog_fetch_timeout_seconds)
}

/// Request header carrying the idempotency key.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";

/// Replay TTL from initial durable Ready publication (physical deletion may lag).
pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Process-local cache/admission budget; Pending entries resist capacity eviction.
pub const IDEMPOTENCY_MAX_ENTRIES: usize = 1024;

/// Per-server-instance delay between idle idempotency maintenance passes.
pub const IDEMPOTENCY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Independent per-pass limits for legacy registrations, due retirements and temp entries.
pub const IDEMPOTENCY_SWEEP_MAX_ENTRIES: usize = 256;

/// Default bound on `generator_runs_total` series exported by `/metrics`.
/// Deployment policy may set any value within the mechanism bounds; the
/// scrape stays bounded in every case (never unbounded).
pub const DEFAULT_METRICS_GENERATOR_LIMIT: usize = 20;

/// Inclusive mechanism bounds for the metrics generator-series budget.
/// The upper bound keeps one scrape to tens of kilobytes worst case.
pub const MIN_METRICS_GENERATOR_LIMIT: usize = 1;
pub const MAX_METRICS_GENERATOR_LIMIT: usize = 256;

/// Inclusive mechanism bound for pinned generator selections carried in
/// `METRICS_PINNED_GENERATORS`. Keeps the parsed env value bounded.
pub const MAX_METRICS_PINNED_GENERATORS: usize = 64;

/// Inclusive mechanism bound for one pinned generator id, in bytes.
pub const MAX_METRICS_PINNED_ID_BYTES: usize = 256;

/// Default bound on tracked rate-limit identity buckets. The bucket
/// mechanism stays bounded in every case; deployments may set any value
/// within the mechanism bounds.
pub const DEFAULT_RATE_LIMIT_MAX_IDENTITIES: usize = 4_096;

/// Inclusive mechanism bounds for the rate-limit identity budget. The upper
/// bound keeps the bucket map to a few megabytes worst case (one small
/// bucket per identity) instead of unbounded growth.
pub const MIN_RATE_LIMIT_MAX_IDENTITIES: usize = 1;
pub const MAX_RATE_LIMIT_MAX_IDENTITIES: usize = 65_536;

/// Maximum bytes kept per tool-call event value before truncation.
pub const TOOL_EVENT_VALUE_LIMIT_BYTES: usize = 32 * 1024;

/// Shipped per-value detail limit for direct `mcp.call` events. This path
/// historically retained 64 KiB while the shared tool-call ceiling is
/// `TOOL_EVENT_VALUE_LIMIT_BYTES`; a configured `max_tool_value_bytes`
/// lowers either path.
pub const DEFAULT_DIRECT_MCP_TOOL_VALUE_BYTES: usize = 64 * 1024;

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

/// Strict bounded-integer env parse: out-of-range and unparseable values
/// refuse boot with the variable name, its bounds, and the actual value.
pub fn parse_bounded_env(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(value) => match value.parse::<u64>() {
            Ok(parsed) if (min..=max).contains(&parsed) => Ok(parsed),
            _ => Err(format!(
                "invalid {name} `{value}`: must be an integer between {min} and {max}"
            )),
        },
    }
}

/// Strict positive-integer env parse: invalid, zero, and negative values
/// refuse boot with an error naming the variable (E04).
pub fn parse_positive_env(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed >= 1 => Ok(parsed),
            _ => Err(format!(
                "invalid {name} `{value}`: must be an integer of at least 1"
            )),
        },
    }
}

#[cfg(test)]
mod operational_limits_tests {
    use super::*;

    #[test]
    fn defaults_match_today_values() {
        assert_eq!(
            OperationalLimits::default(),
            OperationalLimits {
                agent_tool_calls: 10,
                agent_iterations: 32,
                fallback_models: 8,
                web_search_results: 20,
                retry_attempts: 16,
                retry_backoff_ms: 60_000,
                provider_request_timeout_seconds: 120,
                catalog_fetch_timeout_seconds: 30,
                patch_target_bytes: 8 * 1024 * 1024,
                audit_tool_value_bytes: 32 * 1024,
                direct_mcp_tool_value_bytes: 64 * 1024,
            }
        );
        assert_eq!(
            serde_json::from_str::<OperationalLimits>("{}").unwrap(),
            OperationalLimits::default()
        );
    }

    #[test]
    fn unknown_keys_are_refused() {
        let error = serde_json::from_str::<OperationalLimits>(r#"{"max_calls": 4}"#)
            .expect_err("strict config must reject unknown keys");
        assert!(error.to_string().contains("max_calls"), "{error}");
    }

    #[test]
    fn timeout_resolution_preserves_defaults_and_honours_overrides() {
        let defaults = OperationalLimits::default();
        assert_eq!(
            effective_provider_timeout_seconds(None, &defaults),
            DEFAULT_PROVIDER_REQUEST_TIMEOUT_SECONDS
        );
        assert_eq!(catalog_fetch_timeout(&defaults), Duration::from_secs(30));

        // Same shipped values whether the section is absent or empty.
        let empty = OperationalLimits {
            ..OperationalLimits::default()
        };
        assert_eq!(effective_provider_timeout_seconds(None, &empty), 120);
        assert_eq!(catalog_fetch_timeout(&empty), Duration::from_secs(30));

        for (seconds, catalog) in [(1, 1), (600, 120)] {
            let limits = OperationalLimits {
                provider_request_timeout_seconds: seconds,
                catalog_fetch_timeout_seconds: catalog,
                ..OperationalLimits::default()
            };
            limits
                .validate()
                .expect("both timeout values must be valid");
            assert_eq!(effective_provider_timeout_seconds(None, &limits), seconds);
            assert_eq!(catalog_fetch_timeout(&limits), Duration::from_secs(catalog));
            // An explicit row value always wins over the deployment default.
            assert_eq!(effective_provider_timeout_seconds(Some(7), &limits), 7);
        }
    }

    #[test]
    fn deployment_ceiling_is_frozen_for_the_process() {
        let narrowed = OperationalLimits {
            agent_tool_calls: 3,
            retry_attempts: 4,
            ..OperationalLimits::default()
        };
        set_deployment_operational_limits(narrowed).expect("first installation wins");
        set_deployment_operational_limits(narrowed)
            .expect("repeated registry parsing must not change policy");
        assert_eq!(operational_limits().agent_tool_calls, 3);
        assert_eq!(operational_limits().retry_attempts, 4);
        let conflicting = OperationalLimits {
            agent_tool_calls: 4,
            ..narrowed
        };
        assert!(set_deployment_operational_limits(conflicting).is_err());
    }

    #[test]
    fn values_may_narrow_or_raise_but_not_disable() {
        let narrowed = OperationalLimits {
            agent_tool_calls: 2,
            patch_target_bytes: 1024,
            audit_tool_value_bytes: 1024,
            direct_mcp_tool_value_bytes: 1024,
            ..OperationalLimits::default()
        };
        narrowed.validate().expect("narrowing is allowed");

        // The configured values are the effective ceilings: raising above the
        // defaults is valid and lets contracts declare more than today.
        for json in [
            r#"{"agent_tool_calls": 11}"#,
            r#"{"agent_tool_calls": 20}"#,
            r#"{"agent_iterations": 64}"#,
            r#"{"fallback_models": 16}"#,
            r#"{"web_search_results": 40}"#,
            r#"{"retry_attempts": 32}"#,
            r#"{"retry_backoff_ms": 120000}"#,
            r#"{"provider_request_timeout_seconds": 600}"#,
            r#"{"catalog_fetch_timeout_seconds": 120}"#,
            r#"{"patch_target_bytes": 16777216}"#,
            r#"{"audit_tool_value_bytes": 65536}"#,
            r#"{"direct_mcp_tool_value_bytes": 131072}"#,
        ] {
            let limits: OperationalLimits = serde_json::from_str(json).unwrap();
            limits
                .validate()
                .expect("raising above the defaults must be allowed");
        }

        // Zero counts (and a zero patch target) stay refused; zero backoff
        // remains valid because it means no delay.
        for (json, expected) in [
            (r#"{"agent_tool_calls": 0}"#, "agent_tool_calls"),
            (r#"{"agent_iterations": 0}"#, "agent_iterations"),
            (r#"{"fallback_models": 0}"#, "fallback_models"),
            (r#"{"web_search_results": 0}"#, "web_search_results"),
            (r#"{"retry_attempts": 0}"#, "retry_attempts"),
            (
                r#"{"provider_request_timeout_seconds": 0}"#,
                "provider_request_timeout_seconds",
            ),
            (
                r#"{"catalog_fetch_timeout_seconds": 0}"#,
                "catalog_fetch_timeout_seconds",
            ),
            (r#"{"patch_target_bytes": 0}"#, "patch_target_bytes"),
            (r#"{"audit_tool_value_bytes": 0}"#, "audit_tool_value_bytes"),
            (
                r#"{"direct_mcp_tool_value_bytes": 0}"#,
                "direct_mcp_tool_value_bytes",
            ),
        ] {
            let limits: OperationalLimits = serde_json::from_str(json).unwrap();
            let error = limits.validate().expect_err("zero value must be refused");
            assert!(error.contains(expected), "{error}");
        }
        let zero_backoff: OperationalLimits =
            serde_json::from_str(r#"{"retry_backoff_ms": 0}"#).unwrap();
        zero_backoff
            .validate()
            .expect("zero backoff means no delay and stays valid");
    }
}

#[cfg(test)]
mod bool_env_tests {
    use super::env_process;
    use super::parse_bool_env;

    #[test]
    fn bool_knobs_reject_garbage_instead_of_folding() {
        if env_process::isolated() {
            return;
        }
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
                std::env::set_var("POLICY_BOOL_TEST", value);
            }
            assert_eq!(
                parse_bool_env("POLICY_BOOL_TEST", true),
                Ok(expected),
                "value `{value}` must parse"
            );
            unsafe {
                std::env::remove_var("POLICY_BOOL_TEST");
            }
        }
        for garbage in ["2", "yes please", "", "enabled", "tru"] {
            unsafe {
                std::env::set_var("POLICY_BOOL_TEST", garbage);
            }
            let error = parse_bool_env("POLICY_BOOL_TEST", true).expect_err("garbage must fail");
            assert!(
                error.contains("POLICY_BOOL_TEST"),
                "the error must name the variable: {error}"
            );
            unsafe {
                std::env::remove_var("POLICY_BOOL_TEST");
            }
        }
        unsafe {
            std::env::remove_var("POLICY_BOOL_TEST");
        }
        assert_eq!(parse_bool_env("POLICY_BOOL_TEST", true), Ok(true));
        assert_eq!(parse_bool_env("POLICY_BOOL_TEST", false), Ok(false));
    }
}

#[cfg(test)]
mod env_process {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/test-support/environment.rs"
    ));
}
