use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct NodePath(String);

impl NodePath {
    pub fn root(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn foreach_child(&self, index: usize, child: &str) -> Self {
        Self(format!("{}[{index}]/{child}", self.0))
    }

    pub fn repair_child(&self, attempt: u32, child: &str) -> Self {
        Self(format!("{}@repair.{attempt}/{child}", self.0))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
