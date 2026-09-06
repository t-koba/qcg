mod ask_user;
mod await_step;
mod check_command;
mod check_container;
mod check_tool;
mod checks;
mod command;
mod common;
mod container_backend;
mod fail;
mod files;
mod foreach;
mod http;
mod mcp_call;
mod transform;

use ask_user::AskUserStep;
use await_step::AwaitStep;
use check_command::CheckCommandStep;
use check_container::CheckContainerStep;
use check_tool::CheckToolStep;
use checks::{CheckContractStep, CheckFormatStep, CheckSchemaStep};
use command::CommandStep;
use fail::FailStep;
use files::{CopyStep, RenderStep, WriteStep};
use foreach::ForeachStep;
use http::HttpStep;
use mcp_call::McpCallStep;
use qcg_engine::StepRegistry;
use qcg_mcp::McpRuntime;
use std::sync::Arc;
use transform::TransformStep;

pub fn deterministic_registry() -> StepRegistry {
    deterministic_registry_with_mcp(Arc::new(McpRuntime::public_defaults()))
}

pub fn deterministic_registry_with_mcp(mcp: Arc<McpRuntime>) -> StepRegistry {
    let mut registry = StepRegistry::new();
    registry.register(RenderStep);
    registry.register(WriteStep);
    registry.register(CopyStep);
    registry.register(TransformStep);
    registry.register(CommandStep);
    registry.register(HttpStep);
    registry.register(McpCallStep { runtime: mcp });
    registry.register(AskUserStep);
    registry.register(CheckSchemaStep);
    registry.register(CheckFormatStep);
    registry.register(CheckCommandStep);
    registry.register(CheckToolStep);
    registry.register(CheckContainerStep);
    registry.register(CheckContractStep);
    registry.register(ForeachStep);
    registry.register(FailStep);
    registry.register(AwaitStep);
    registry
}
