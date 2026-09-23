use qcg_policy::{DEFAULT_MCP_MAX_RESPONSE_BYTES, DEFAULT_MCP_TIMEOUT_SECONDS};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    StreamableHttp,
    Stdio,
}

pub(crate) fn default_transport() -> McpTransport {
    McpTransport::StreamableHttp
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpLifecycle {
    Initialize,
    Discover,
}

pub(crate) fn default_lifecycle() -> McpLifecycle {
    McpLifecycle::Discover
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpAuth {
    None,
    Bearer,
    Header,
    Oauth,
}

pub(crate) fn default_auth() -> McpAuth {
    McpAuth::None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthCredentialStore {
    Keyring,
    Memory,
}

pub(crate) fn default_oauth_store() -> OAuthCredentialStore {
    OAuthCredentialStore::Keyring
}

pub(crate) fn default_timeout_seconds() -> u64 {
    DEFAULT_MCP_TIMEOUT_SECONDS
}

pub(crate) fn default_max_response_bytes() -> usize {
    DEFAULT_MCP_MAX_RESPONSE_BYTES
}

pub(crate) const DEFAULT_TOOLS_LIST_PAGE_LIMIT: usize = 100;

pub(crate) const DEFAULT_OAUTH_STATE_TTL_SECONDS: u64 = 10 * 60;

pub(crate) const DEFAULT_TASK_POLL_INTERVAL_MS: u64 = 250;

pub(crate) fn default_tools_list_page_limit() -> usize {
    DEFAULT_TOOLS_LIST_PAGE_LIMIT
}

pub(crate) fn default_oauth_state_ttl_seconds() -> u64 {
    DEFAULT_OAUTH_STATE_TTL_SECONDS
}

pub(crate) fn default_task_poll_interval_ms() -> u64 {
    DEFAULT_TASK_POLL_INTERVAL_MS
}
