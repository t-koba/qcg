//! Policy layer: pure validation, bounds, and schema helpers shared by every
//! crate. Transport and execution code must not reimplement these; resolve
//! policy here and pass plain values down.

pub mod cost;
pub mod credential;
pub mod limits;
pub mod params;
pub mod path;
pub mod schema;

pub use cost::{LlmCostBudget, PricingRow, select_pricing};
pub use credential::credential_like_name;
pub use limits::{
    DEFAULT_LLM_CONTEXT_LIMIT_BYTES, DEFAULT_MAX_ACTIVE_RUNS, DEFAULT_MAX_TOTAL_STEPS,
    DEFAULT_MAX_TRACKED_RUNS, DEFAULT_MCP_MAX_RESPONSE_BYTES, DEFAULT_MCP_TIMEOUT_SECONDS,
    GENERATED_PACKAGE_ENTRIES, IDEMPOTENCY_HEADER, IDEMPOTENCY_MAX_ENTRIES, IDEMPOTENCY_TTL,
    JOURNAL_POLL_CHANNEL_CAPACITY, MAX_COMMAND_RESULT_FILES, MAX_CONFIRM_INPUT_BYTES,
    MAX_CREDENTIAL_FILE_BYTES, MAX_DIRECTORY_SCAN_ENTRIES, MAX_FOREACH_ITERATIONS,
    MAX_FOREACH_PARALLELISM, MAX_INTERACTIVE_INPUT_BYTES, MAX_SIGNING_KEY_BYTES,
    TOOL_EVENT_VALUE_LIMIT_BYTES,
};
pub use params::{params_schema, string_array_schema, string_schema};
pub use path::{is_safe_relative_path, portable_relative_path};
pub use schema::{
    MAX_JSON_SCHEMA_BYTES, MAX_JSON_SCHEMA_DEPTH, MAX_JSON_SCHEMA_NODES,
    MAX_JSON_SCHEMA_OBJECT_MEMBERS, MAX_JSON_SCHEMA_STRING_BYTES, compile_bounded_validator,
    validate_bounded_json_schema,
};
