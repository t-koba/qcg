use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("{0}")]
    Configuration(String),
    #[error("MCP server `{server}` requires OAuth authorization; connect it from the qcg web UI")]
    AuthorizationRequired { server: String },
    #[error("MCP authorization failed: {0}")]
    Authorization(String),
    #[error("MCP transport failed: {0}")]
    Transport(String),
    #[error("MCP tool `{tool}` returned an error")]
    ToolFailed { tool: String, result: Value },
    #[error("MCP operation timed out after {seconds} seconds")]
    TimedOut { seconds: u64 },
    #[error("MCP operation was canceled")]
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpInputRequired {
    pub input_requests: BTreeMap<String, Value>,
    pub request_state: Option<String>,
}

#[derive(Debug, Clone)]
pub enum McpCallOutcome {
    Complete(Value),
    InputRequired(McpInputRequired),
}
