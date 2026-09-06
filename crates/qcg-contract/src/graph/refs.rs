use crate::manifest::{Manifest, NodeDef};
use qcg_types::{DependencyFailure, DependencyStatus, FailureCode, FailureDetail, NodePath};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::types::NodeState;

pub(crate) fn dependency_failure(
    node: &NodeDef,
    states: &BTreeMap<String, NodeState>,
    code: FailureCode,
) -> Option<FailureDetail> {
    let dependencies = node
        .needs
        .iter()
        .filter_map(|need| match states.get(need) {
            Some(NodeState::Skipped(reason)) => Some(DependencyFailure {
                path: NodePath::root(need),
                status: DependencyStatus::Skipped,
                message: reason.message.clone(),
            }),
            Some(NodeState::Failed(reason)) => Some(DependencyFailure {
                path: NodePath::root(need),
                status: DependencyStatus::Failed,
                message: reason.message.clone(),
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    if dependencies.is_empty() {
        return None;
    }
    let message = if code == FailureCode::NoDependencySucceeded {
        "on_deps=any_succeeded had no successful dependency".into()
    } else {
        dependencies
            .iter()
            .map(|failure| {
                format!(
                    "dependency `{}` {}: {}",
                    failure.path,
                    match failure.status {
                        DependencyStatus::Skipped => "skipped",
                        DependencyStatus::Failed => "failed",
                    },
                    failure.message
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    Some(FailureDetail {
        code,
        message,
        dependencies,
    })
}

pub(crate) fn flow_with_implicit_dependencies(manifest: &Manifest) -> Result<Vec<NodeDef>, String> {
    let mut flow = manifest.flow.clone();
    let parallel_indices = manifest
        .parallel
        .iter()
        .map(|id| {
            flow.iter()
                .position(|node| &node.id == id)
                .ok_or_else(|| format!("parallel references unknown node `{id}`"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut parallel_range = None;
    if !parallel_indices.is_empty() {
        if parallel_indices.len() < 2 {
            return Err("parallel must contain at least two node ids".into());
        }
        let Some(first) = parallel_indices.iter().min().copied() else {
            return Err("parallel must contain at least two node ids".into());
        };
        let Some(last) = parallel_indices.iter().max().copied() else {
            return Err("parallel must contain at least two node ids".into());
        };
        if last - first + 1 != parallel_indices.len() {
            return Err("parallel node ids must be contiguous in flow order".into());
        }
        let declared = parallel_indices.iter().copied().collect::<BTreeSet<_>>();
        if (first..=last).any(|index| !declared.contains(&index)) {
            return Err("parallel node ids must be unique and contiguous".into());
        }
        let anchor = first.checked_sub(1).map(|index| flow[index].id.clone());
        for node in &mut flow[first..=last] {
            if !node.needs.is_empty() {
                return Err(format!(
                    "parallel node `{}` must not also declare needs",
                    node.id
                ));
            }
            if let Some(anchor) = &anchor {
                node.needs.push(anchor.clone());
            }
        }
        if let Some(after) = flow.get_mut(last + 1)
            && after.needs.is_empty()
        {
            after.needs = manifest.parallel.clone();
        }
        parallel_range = Some(first..=last);
    }
    for index in 1..flow.len() {
        if flow[index].needs.is_empty()
            && parallel_range
                .as_ref()
                .is_none_or(|range| !range.contains(&index))
        {
            let previous = flow[index - 1].id.clone();
            flow[index].needs.push(previous);
        }
    }
    Ok(flow)
}

pub(crate) fn validate_on_fail_refs(
    node: &NodeDef,
    nodes: &BTreeMap<String, NodeDef>,
) -> Result<(), String> {
    let Some(on_fail) = &node.on_fail else {
        return Ok(());
    };
    validate_on_fail_ref(node, nodes, on_fail)
}

fn validate_on_fail_ref(
    node: &NodeDef,
    nodes: &BTreeMap<String, NodeDef>,
    on_fail: &crate::manifest::OnFail,
) -> Result<(), String> {
    match on_fail {
        crate::manifest::OnFail::Repair {
            repair,
            recheck,
            on_exhausted,
            ..
        } => {
            if !nodes.contains_key(repair) {
                return Err(format!(
                    "node `{}` on_fail repair references unknown node `{repair}`",
                    node.id
                ));
            }
            if !nodes.contains_key(recheck) {
                return Err(format!(
                    "node `{}` on_fail recheck references unknown node `{recheck}`",
                    node.id
                ));
            }
            validate_on_exhausted_ref(node, nodes, on_exhausted)?;
        }
        crate::manifest::OnFail::Route { to } => {
            if !nodes.contains_key(to) {
                return Err(format!(
                    "node `{}` on_fail route references unknown node `{to}`",
                    node.id
                ));
            }
        }
        crate::manifest::OnFail::Regenerate { on_exhausted, .. } => {
            validate_on_exhausted_ref(node, nodes, on_exhausted)?;
        }
        crate::manifest::OnFail::AskUser | crate::manifest::OnFail::Fail => {}
    }
    Ok(())
}

fn validate_on_exhausted_ref(
    node: &NodeDef,
    nodes: &BTreeMap<String, NodeDef>,
    on_exhausted: &crate::manifest::ExhaustedAction,
) -> Result<(), String> {
    match on_exhausted {
        crate::manifest::ExhaustedAction::Fail
        | crate::manifest::ExhaustedAction::AskUser { .. } => Ok(()),
        crate::manifest::ExhaustedAction::Route { to } => {
            if !nodes.contains_key(to) {
                return Err(format!(
                    "node `{}` on_fail on_exhausted route references unknown node `{to}`",
                    node.id
                ));
            }
            Ok(())
        }
    }
}

pub(crate) fn topo_sort(nodes: &BTreeMap<String, NodeDef>) -> Result<Vec<String>, String> {
    let mut indegree = BTreeMap::<String, usize>::new();
    let mut outgoing = BTreeMap::<String, Vec<String>>::new();
    for id in nodes.keys() {
        indegree.insert(id.clone(), 0);
    }
    for node in nodes.values() {
        for need in &node.needs {
            *indegree.entry(node.id.clone()).or_default() += 1;
            outgoing
                .entry(need.clone())
                .or_default()
                .push(node.id.clone());
        }
    }
    let mut queue: VecDeque<String> = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
        .collect();
    let mut order = Vec::new();
    while let Some(id) = queue.pop_front() {
        order.push(id.clone());
        for next in outgoing.get(&id).into_iter().flatten() {
            let degree = indegree
                .get_mut(next)
                .ok_or_else(|| format!("internal graph error for `{next}`"))?;
            *degree -= 1;
            if *degree == 0 {
                queue.push_back(next.clone());
            }
        }
    }
    if order.len() != nodes.len() {
        let seen: BTreeSet<_> = order.into_iter().collect();
        let cycle_nodes: Vec<_> = nodes
            .keys()
            .filter(|id| !seen.contains(*id))
            .cloned()
            .collect();
        return Err(format!(
            "cycle detected involving {}",
            cycle_nodes.join(", ")
        ));
    }
    Ok(order)
}
