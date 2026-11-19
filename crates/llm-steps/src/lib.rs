mod agent;
mod agent_runtime;
mod agent_tools;
mod catalog_step;
mod choose;
mod completion;
mod context;
mod decide;
mod generate;
mod guardrail;
mod mcp_forms;
mod mcp_tools;
mod out_of_contract;
mod output;
mod prompting;
mod repair;
mod request;
mod request_policy;
mod routes;
mod schemas;
mod search;
mod skill_tool;
mod specialist;
mod tool_events;
mod validation;

#[cfg(test)]
pub(crate) use agent::*;
#[cfg(test)]
pub(crate) use agent_runtime::*;
#[cfg(test)]
pub(crate) use agent_tools::*;
#[cfg(test)]
pub(crate) use completion::*;
#[cfg(test)]
pub(crate) use context::*;
#[cfg(test)]
pub(crate) use guardrail::*;
#[cfg(test)]
pub(crate) use mcp_forms::*;
#[cfg(test)]
pub(crate) use mcp_tools::*;
#[cfg(test)]
pub(crate) use prompting::*;
#[cfg(test)]
pub(crate) use request::*;
#[cfg(test)]
pub(crate) use request_policy::*;
#[cfg(test)]
pub(crate) use search::*;
use std::sync::Arc;
#[cfg(test)]
pub(crate) use tool_events::*;

use engine::StepRegistry;
use llm::LlmRuntime;

use crate::agent::LlmAgentStep;
use crate::choose::LlmChooseStep;
use crate::decide::LlmDecideStep;
use crate::generate::{LlmFillStep, LlmGenerateStep};
use crate::repair::LlmRepairStep;

pub fn register_fake_llm_steps(registry: &mut StepRegistry) {
    register_llm_steps(registry, Arc::new(LlmRuntime::builtins()));
}

pub fn register_llm_steps(registry: &mut StepRegistry, runtime: Arc<LlmRuntime>) {
    registry.reserve_secret_env_names(runtime.provider.credential_env_names());
    registry.reserve_secret_env_names(runtime.search.credential_env_names());
    registry.reserve_secret_env_names(runtime.mcp.credential_env_names());
    registry.register(LlmGenerateStep {
        runtime: Arc::clone(&runtime),
    });
    registry.register(LlmFillStep {
        runtime: Arc::clone(&runtime),
    });
    registry.register(LlmChooseStep {
        runtime: Arc::clone(&runtime),
    });
    registry.register(LlmDecideStep {
        runtime: Arc::clone(&runtime),
    });
    registry.register(LlmRepairStep {
        runtime: Arc::clone(&runtime),
    });
    registry.register(catalog_step::LlmCatalogStep {
        runtime: Arc::clone(&runtime),
    });
    registry.register(LlmAgentStep { runtime });
}

#[cfg(test)]
mod tests;
