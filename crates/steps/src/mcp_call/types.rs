use mcp::McpRuntime;
use policy::McpAuthorizationAction;
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
    /// Optional transport failures degrade to null; validation, elicitation, and confirmation still fail.
    #[serde(default)]
    pub(crate) optional: bool,
    #[serde(default)]
    pub(crate) on_authorization_required: Option<McpAuthorizationAction>,
    pub(crate) cleanup_failure: Option<api::McpCleanupPolicy>,
}

impl McpCallParams {
    pub(crate) fn cleanup_settlement(&self) -> api::McpCleanupSettlement {
        api::McpCleanupSettlement {
            policy: self
                .cleanup_failure
                .unwrap_or(api::McpCleanupPolicy::PreserveExecution),
            source: if self.cleanup_failure.is_some() {
                api::McpCleanupPolicySource::NodeParameter
            } else {
                api::McpCleanupPolicySource::Default
            },
        }
    }

    pub(crate) fn authorization_action(&self) -> (McpAuthorizationAction, &'static str) {
        match self.on_authorization_required {
            Some(action) => (action, "node_parameter"),
            None => (
                if self.optional {
                    McpAuthorizationAction::Degrade
                } else {
                    McpAuthorizationAction::Fail
                },
                "default",
            ),
        }
    }
}
fn default_true() -> bool {
    true
}
