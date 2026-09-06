use crate::manifest::NodeDef;
use qcg_types::FailureDetail;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Graph {
    pub nodes: BTreeMap<String, NodeDef>,
    pub order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Pending,
    Running,
    Success,
    Skipped(FailureDetail),
    Failed(FailureDetail),
}
