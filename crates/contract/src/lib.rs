mod agent;
pub mod expr;
mod graph;
mod manifest;
mod path;
mod schema;
mod skill;

pub use agent::{AgentFailureAction, AgentFailureCode, RecoverableAgentFailureCode};
pub use expr::{Expr, ValueBag};
pub use graph::{Graph, NodeState};
pub use manifest::*;
pub use path::*;
pub use schema::{AssetSpec, FieldType, GeneratorMeta, InputField, InputSpec, InputStage};
pub use skill::*;

/// Canonical manifest file name inside a generator directory.
/// Renaming the manifest requires changing only this constant.
pub const MANIFEST_FILE: &str = "qcg.toml";
