use crate::{AgentFailureAction, AgentFailureCode, RecoverableAgentFailureCode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::llm::{LlmRequestPolicy, ModelRef};
use super::nodes::default_true;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ToolDecl {
    #[serde(rename = "fs.write")]
    FsWrite {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        path_prefix: String,
    },
    #[serde(rename = "command")]
    Command {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        command: Vec<String>,
    },
    #[serde(rename = "http")]
    Http {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        methods: Vec<String>,
        hosts: Vec<String>,
    },
    #[serde(rename = "ask_user")]
    AskUser {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
    },
    #[serde(rename = "web.search")]
    WebSearch {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        provider: Option<String>,
        #[serde(default = "default_search_max_results")]
        max_results: usize,
        #[serde(default = "default_search_max_calls")]
        max_calls: usize,
    },
    #[serde(rename = "mcp")]
    Mcp {
        name: String,
        #[serde(default)]
        description: Option<String>,
        server: String,
        tool: String,
        #[serde(default = "default_mcp_max_calls")]
        max_calls: usize,
        #[serde(default = "default_true")]
        side_effects: bool,
    },
    #[serde(rename = "agent")]
    Agent {
        name: String,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        input_schema: Option<Value>,
        #[serde(default)]
        output_schema: Option<String>,
        instructions: String,
        #[serde(default)]
        tools: Vec<String>,
        #[serde(default = "default_agent_tool_max_calls")]
        max_calls: usize,
        #[serde(default = "default_agent_tool_max_iterations")]
        max_iterations: usize,
        #[serde(default = "default_agent_tool_max_tokens_total")]
        max_tokens_total: u64,
        max_tool_calls_total: usize,
        #[serde(default)]
        model: Option<ModelRef>,
        #[serde(default)]
        fallback_models: Vec<ModelRef>,
        #[serde(default)]
        request: Box<LlmRequestPolicy>,
        #[serde(default)]
        on_failure: Box<AgentFailurePolicy>,
        #[serde(default)]
        handoff: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentFailurePolicy {
    #[serde(default)]
    pub default: AgentFailureAction,
    #[serde(default)]
    pub by_code: BTreeMap<RecoverableAgentFailureCode, AgentFailureAction>,
}

impl Default for AgentFailurePolicy {
    fn default() -> Self {
        Self {
            default: AgentFailureAction::ReturnError,
            by_code: BTreeMap::new(),
        }
    }
}

impl AgentFailurePolicy {
    pub fn action(&self, code: AgentFailureCode) -> AgentFailureAction {
        code.policy_code()
            .and_then(|code| self.by_code.get(&code).copied())
            .unwrap_or_else(|| {
                if code.is_recoverable() {
                    self.default
                } else {
                    AgentFailureAction::Fail
                }
            })
    }
}

impl ToolDecl {
    pub fn name(&self) -> &str {
        match self {
            Self::FsWrite { name, .. }
            | Self::Command { name, .. }
            | Self::Http { name, .. }
            | Self::AskUser { name, .. }
            | Self::WebSearch { name, .. }
            | Self::Mcp { name, .. }
            | Self::Agent { name, .. } => name,
        }
    }

    pub fn description(&self) -> Option<&str> {
        match self {
            Self::FsWrite { description, .. }
            | Self::Command { description, .. }
            | Self::Http { description, .. }
            | Self::AskUser { description, .. }
            | Self::WebSearch { description, .. }
            | Self::Mcp { description, .. }
            | Self::Agent { description, .. } => description.as_deref(),
        }
    }

    pub fn input_schema(&self) -> Option<&Value> {
        match self {
            Self::FsWrite { input_schema, .. }
            | Self::Command { input_schema, .. }
            | Self::Http { input_schema, .. }
            | Self::AskUser { input_schema, .. }
            | Self::Agent { input_schema, .. } => input_schema.as_ref(),
            Self::WebSearch { .. } | Self::Mcp { .. } => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::FsWrite { .. } => "fs.write",
            Self::Command { .. } => "command",
            Self::Http { .. } => "http",
            Self::AskUser { .. } => "ask_user",
            Self::WebSearch { .. } => "web.search",
            Self::Mcp { .. } => "mcp",
            Self::Agent { .. } => "agent",
        }
    }
}

fn default_search_max_results() -> usize {
    5
}

fn default_search_max_calls() -> usize {
    3
}

fn default_mcp_max_calls() -> usize {
    3
}

fn default_agent_tool_max_iterations() -> usize {
    6
}

fn default_agent_tool_max_calls() -> usize {
    3
}

fn default_agent_tool_max_tokens_total() -> u64 {
    32_768
}
