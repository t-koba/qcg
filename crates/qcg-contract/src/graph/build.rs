use crate::manifest::{Manifest, NodeDef, OnDeps};
use qcg_types::{FailureCode, FailureDetail};
use std::collections::BTreeMap;

use super::refs::{
    dependency_failure, flow_with_implicit_dependencies, topo_sort, validate_on_fail_refs,
};
use super::types::{Graph, NodeState};

impl Graph {
    pub fn build(manifest: &Manifest) -> Result<Self, String> {
        let flow = flow_with_implicit_dependencies(manifest)?;
        let mut nodes = BTreeMap::<String, NodeDef>::new();
        for node in flow {
            if nodes.insert(node.id.clone(), node.clone()).is_some() {
                return Err(format!("duplicate node `{}`", node.id));
            }
        }
        for node in nodes.values() {
            for need in &node.needs {
                if !nodes.contains_key(need) {
                    return Err(format!(
                        "node `{}` depends on unknown node `{need}`",
                        node.id
                    ));
                }
            }
            validate_on_fail_refs(node, &nodes)?;
            if node.kind.as_str() == "foreach" {
                let subflow = node
                    .param_str("subflow")
                    .ok_or_else(|| format!("foreach node `{}` must declare subflow", node.id))?;
                if !manifest.blocks.contains_key(subflow) {
                    return Err(format!(
                        "foreach node `{}` references unknown block `{subflow}`",
                        node.id
                    ));
                }
            }
        }
        let order = topo_sort(&nodes)?;
        let graph = Self { nodes, order };
        graph.warn_unreachable();
        Ok(graph)
    }

    pub fn roots(&self) -> Vec<&NodeDef> {
        self.nodes
            .values()
            .filter(|node| node.needs.is_empty())
            .collect()
    }

    fn warn_unreachable(&self) {
        let _ = self;
    }

    pub fn needs_satisfied(&self, node: &NodeDef, states: &BTreeMap<String, NodeState>) -> bool {
        if node.needs.is_empty() {
            return true;
        }
        match node.on_deps {
            OnDeps::AllSucceeded => node
                .needs
                .iter()
                .all(|need| matches!(states.get(need), Some(NodeState::Success))),
            OnDeps::AnySucceeded => node
                .needs
                .iter()
                .any(|need| matches!(states.get(need), Some(NodeState::Success))),
            OnDeps::NoneFailed => node.needs.iter().all(|need| {
                matches!(
                    states.get(need),
                    Some(NodeState::Success | NodeState::Skipped(_))
                )
            }),
        }
    }

    pub fn should_skip_by_dependencies(
        &self,
        node: &NodeDef,
        states: &BTreeMap<String, NodeState>,
    ) -> Option<FailureDetail> {
        if node.needs.is_empty() {
            return None;
        }
        match node.on_deps {
            OnDeps::AllSucceeded => {
                dependency_failure(node, states, FailureCode::DependencyUnsatisfied)
            }
            OnDeps::AnySucceeded => {
                let all_terminal = node.needs.iter().all(|need| {
                    matches!(
                        states.get(need),
                        Some(NodeState::Success | NodeState::Skipped(_) | NodeState::Failed(_))
                    )
                });
                let any_success = node
                    .needs
                    .iter()
                    .any(|need| matches!(states.get(need), Some(NodeState::Success)));
                if all_terminal && !any_success {
                    dependency_failure(node, states, FailureCode::NoDependencySucceeded)
                } else {
                    None
                }
            }
            OnDeps::NoneFailed => {
                let all_terminal = node.needs.iter().all(|need| {
                    matches!(
                        states.get(need),
                        Some(NodeState::Success | NodeState::Skipped(_) | NodeState::Failed(_))
                    )
                });
                let any_failed = node
                    .needs
                    .iter()
                    .any(|need| matches!(states.get(need), Some(NodeState::Failed(_))));
                if all_terminal && any_failed {
                    dependency_failure(node, states, FailureCode::DependencyUnsatisfied)
                } else {
                    None
                }
            }
        }
    }
}
