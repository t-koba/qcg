//! Policy layer: pure validation, bounds, and schema helpers shared by every
//! crate. Transport and execution code must not reimplement these; resolve
//! policy here and pass plain values down.

pub mod audit;
pub mod cost;
pub mod credential;
pub mod limits;
pub mod params;
pub mod path;
pub mod schema;

pub use audit::{
    AuditConfig, AuditFloor, AuditLevel, AuditLimits, AuditMode, AuditPolicy, EventClass,
    OBSERVATION_EVENT_KINDS, event_class,
};
pub use cost::{LlmCostBudget, PricingRow, select_pricing};
pub use credential::{
    credential_like_name, redact_all_query_values, redact_credential_assignments_in_text,
    redact_header_values, redact_urls_in_text,
};
pub use limits::{
    DEFAULT_GC_INTERVAL_SECS, DEFAULT_GC_KEEP, DEFAULT_GC_KEEP_FAILED,
    DEFAULT_JOURNAL_SCAN_WINDOW_BYTES, DEFAULT_LLM_CONTEXT_LIMIT_BYTES, DEFAULT_MAX_ACTIVE_RUNS,
    DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES, DEFAULT_MAX_TOTAL_STEPS, DEFAULT_MAX_TRACKED_RUNS,
    DEFAULT_MCP_MAX_RESPONSE_BYTES, DEFAULT_MCP_TIMEOUT_SECONDS, DEFAULT_PATCH_BYTES,
    DEFAULT_PATCH_EDITS, GENERATED_PACKAGE_ENTRIES, IDEMPOTENCY_HEADER, IDEMPOTENCY_MAX_ENTRIES,
    IDEMPOTENCY_TTL, JOURNAL_POLL_CHANNEL_CAPACITY, JOURNAL_POLL_INTERVAL_MILLIS,
    LIVE_EVENT_CHANNEL_CAPACITY, MAX_AGENT_ITERATIONS, MAX_AGENT_TOOL_CALLS,
    MAX_ANCHORED_READ_LINES, MAX_COMMAND_RESULT_FILES, MAX_CONFIRM_INPUT_BYTES,
    MAX_CREDENTIAL_FILE_BYTES, MAX_DIRECTORY_SCAN_ENTRIES, MAX_FALLBACK_MODELS,
    MAX_FOREACH_ITERATIONS, MAX_FOREACH_PARALLELISM, MAX_INTERACTIVE_INPUT_BYTES,
    MAX_JOURNAL_POLL_INTERVAL_MILLIS, MAX_JOURNAL_SCAN_WINDOW_BYTES,
    MAX_LIVE_EVENT_CHANNEL_CAPACITY, MAX_PATCH_BYTES, MAX_PATCH_EDITS, MAX_RETRY_ATTEMPTS,
    MAX_RETRY_BACKOFF_MS, MAX_RUN_LABEL_BYTES, MAX_RUN_LABELS, MAX_SHUTDOWN_PHASE_SECS,
    MAX_SIGNING_KEY_BYTES, MAX_STORE_RESCAN_SECS, MAX_WEB_SEARCH_RESULTS,
    MIN_DIRECTORY_SCAN_ENTRIES, MIN_JOURNAL_POLL_INTERVAL_MILLIS, MIN_JOURNAL_SCAN_WINDOW_BYTES,
    MIN_LIVE_EVENT_CHANNEL_CAPACITY, MIN_PATCH_BYTES, MIN_PATCH_EDITS, MIN_SHUTDOWN_PHASE_SECS,
    MIN_STORE_RESCAN_SECS, TOOL_EVENT_VALUE_LIMIT_BYTES, parse_bool_env,
};
pub use params::{params_schema, string_array_schema, string_schema};
pub use path::{is_safe_relative_path, portable_relative_path};
pub use schema::{
    MAX_JSON_SCHEMA_BYTES, MAX_JSON_SCHEMA_DEPTH, MAX_JSON_SCHEMA_NODES,
    MAX_JSON_SCHEMA_OBJECT_MEMBERS, MAX_JSON_SCHEMA_STRING_BYTES, compile_bounded_validator,
    validate_bounded_json_schema,
};
