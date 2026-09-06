use qcg_mcp::McpRuntime;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

pub(crate) struct McpCallStep {
    pub(crate) runtime: Arc<McpRuntime>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct McpCallParams {
    pub(crate) server: String,
    pub(crate) tool: String,
    #[serde(default)]
    pub(crate) arguments: Value,
    #[serde(default)]
    pub(crate) input_schema: Option<Value>,
    #[serde(default)]
    pub(crate) output_schema: Option<Value>,
    #[serde(default)]
    pub(crate) timeout_seconds: Option<u64>,
    #[serde(default = "default_true")]
    pub(crate) side_effects: bool,
    /// Degrade transport failures to a null output instead of failing the run.
    /// Validation, elicitation, and confirmation paths still fail.
    #[serde(default)]
    pub(crate) optional: bool,
}

fn default_true() -> bool {
    true
}
