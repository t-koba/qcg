//! Canonical numeric policy bounds shared by every layer.
//!
//! Each bound lives here exactly once. Crates must reference these items
//! instead of redeclaring values locally, so policy changes stay consistent
//! and reviewable in one place. `None`-style opt-outs stay with the caller:
//! these constants only fix the numeric policy, never its interpretation.

use std::time::Duration;

/// Maximum directory entries scanned when listing run or generator roots.
pub const MAX_DIRECTORY_SCAN_ENTRIES: usize = 100_000;

/// Maximum bytes read from a credential or secret file.
pub const MAX_CREDENTIAL_FILE_BYTES: u64 = 64 * 1024;

/// Inclusive upper bound for `foreach` iteration counts.
pub const MAX_FOREACH_ITERATIONS: usize = 10_000;

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
