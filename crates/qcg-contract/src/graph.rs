use crate::manifest::{Manifest, NodeDef, OnDeps};
use qcg_types::{DependencyFailure, DependencyStatus, FailureCode, FailureDetail, NodePath};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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

fn dependency_failure(
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

fn flow_with_implicit_dependencies(manifest: &Manifest) -> Result<Vec<NodeDef>, String> {
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

fn validate_on_fail_refs(node: &NodeDef, nodes: &BTreeMap<String, NodeDef>) -> Result<(), String> {
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
            if let Some(on_exhausted) = on_exhausted {
                validate_repair_on_exhausted_ref(node, nodes, on_exhausted)?;
            }
        }
        crate::manifest::OnFail::Route { to } => {
            if !nodes.contains_key(to) {
                return Err(format!(
                    "node `{}` on_fail route references unknown node `{to}`",
                    node.id
                ));
            }
        }
        crate::manifest::OnFail::Regenerate { .. }
        | crate::manifest::OnFail::AskUser
        | crate::manifest::OnFail::Fail => {}
    }
    Ok(())
}

fn validate_repair_on_exhausted_ref(
    node: &NodeDef,
    nodes: &BTreeMap<String, NodeDef>,
    on_exhausted: &crate::manifest::RepairExhausted,
) -> Result<(), String> {
    match on_exhausted {
        crate::manifest::RepairExhausted::Fail => Ok(()),
        crate::manifest::RepairExhausted::Route { to } => {
            if !nodes.contains_key(to) {
                return Err(format!(
                    "node `{}` on_fail repair on_exhausted route references unknown node `{to}`",
                    node.id
                ));
            }
            Ok(())
        }
    }
}

fn topo_sort(nodes: &BTreeMap<String, NodeDef>) -> Result<Vec<String>, String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{GeneratorMeta, Manifest, OnDeps, OutputSpec, Permissions, StepType};

    #[test]
    fn rejects_cycles() {
        let manifest = Manifest {
            generator: GeneratorMeta {
                id: "x".into(),
                name: "".into(),
                version: "0.1.0".into(),
                description: "".into(),
                authors: vec![],
                qcg_version: "".into(),
            },
            llm: None,
            inputs: Default::default(),
            resources: Default::default(),
            tools: Default::default(),
            permissions: Permissions::default(),
            secrets: Default::default(),
            runtime: Default::default(),
            budget: Default::default(),
            flow: vec![
                NodeDef {
                    id: "a".into(),
                    kind: StepType::from("write"),
                    needs: vec!["b".into()],
                    when: None,
                    on_deps: Default::default(),
                    context: vec![],
                    output: None,
                    artifact: None,
                    on_fail: None,
                    failure: None,
                    params: Default::default(),
                },
                NodeDef {
                    id: "b".into(),
                    kind: StepType::from("write"),
                    needs: vec!["a".into()],
                    when: None,
                    on_deps: Default::default(),
                    context: vec![],
                    output: None,
                    artifact: None,
                    on_fail: None,
                    failure: None,
                    params: Default::default(),
                },
            ],
            parallel: Vec::new(),
            blocks: Default::default(),
            outputs: OutputSpec::default(),
            failure: Default::default(),
            journal: Default::default(),
            assets: Default::default(),
        };
        assert!(
            Graph::build(&manifest)
                .unwrap_err()
                .contains("cycle detected")
        );
    }

    #[test]
    fn rejects_unsupported_repair_on_exhausted_strategy() {
        let error = toml::from_str::<Manifest>(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "check"
type = "check.format"
on_fail = { action = "repair", repair = "repair", recheck = "recheck", max_attempts = 1, on_exhausted = { action = "ask_user" } }

[[flow]]
id = "repair"
type = "write"

[[flow]]
id = "recheck"
type = "check.format"
"#,
        )
        .expect_err("ask_user is not a valid exhaustion action");
        assert!(
            error.to_string().contains("unknown variant `ask_user`"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn join_all_requires_every_dependency_to_succeed() {
        let graph = empty_graph();
        for (left_name, left, right_name, right, expected) in dependency_state_pairs() {
            let states = state_map(left, right);
            let node = join_node(OnDeps::AllSucceeded);
            assert_eq!(
                graph.needs_satisfied(&node, &states),
                expected == ExpectedJoin::AllSatisfied,
                "left={left_name} right={right_name}"
            );
        }
    }

    #[test]
    fn join_any_runs_after_any_success_and_skips_after_no_success() {
        let graph = empty_graph();
        let node = join_node(OnDeps::AnySucceeded);
        for (left_name, left, right_name, right, expected) in dependency_state_pairs() {
            let states = state_map(left, right);
            assert_eq!(
                graph.needs_satisfied(&node, &states),
                matches!(
                    expected,
                    ExpectedJoin::AllSatisfied | ExpectedJoin::AnySatisfied
                ),
                "left={left_name} right={right_name}"
            );
            let skip = graph.should_skip_by_dependencies(&node, &states);
            if expected == ExpectedJoin::AnyExhausted {
                assert_eq!(
                    skip.as_deref(),
                    Some("on_deps=any_succeeded had no successful dependency"),
                    "left={left_name} right={right_name}"
                );
            } else {
                assert!(skip.is_none(), "left={left_name} right={right_name}");
            }
        }
    }

    #[test]
    fn join_none_failed_treats_skips_as_satisfying_and_failures_as_blocking() {
        let graph = empty_graph();
        let node = join_node(OnDeps::NoneFailed);
        for (left_name, left, right_name, right, _) in dependency_state_pairs() {
            let states = state_map(left.clone(), right.clone());
            let terminal = |state: &NodeState| {
                matches!(
                    state,
                    NodeState::Success | NodeState::Skipped(_) | NodeState::Failed(_)
                )
            };
            let satisfying =
                |state: &NodeState| matches!(state, NodeState::Success | NodeState::Skipped(_));

            let expected_satisfied = satisfying(&left) && satisfying(&right);
            let expected_blocked =
                terminal(&left) && terminal(&right) && (!satisfying(&left) || !satisfying(&right));

            assert_eq!(
                graph.needs_satisfied(&node, &states),
                expected_satisfied,
                "left={left_name} right={right_name}"
            );
            let skip = graph.should_skip_by_dependencies(&node, &states);
            assert_eq!(
                skip.is_some(),
                expected_blocked,
                "left={left_name} right={right_name}"
            );
        }
    }

    #[test]
    fn join_all_reports_every_terminal_dependency_problem() {
        let graph = empty_graph();
        let node = join_node(OnDeps::AllSucceeded);
        let states = state_map(NodeState::Skipped("not needed".into()), NodeState::Success);
        assert_eq!(
            graph.should_skip_by_dependencies(&node, &states).as_deref(),
            Some("dependency `a` skipped: not needed")
        );
        let states = state_map(NodeState::Failed("boom".into()), NodeState::Success);
        assert_eq!(
            graph.should_skip_by_dependencies(&node, &states).as_deref(),
            Some("dependency `a` failed: boom")
        );
        let states = state_map(
            NodeState::Skipped("not needed".into()),
            NodeState::Failed("boom".into()),
        );
        let reason = graph
            .should_skip_by_dependencies(&node, &states)
            .expect("both terminal problems should be reported");
        assert_eq!(reason.dependencies.len(), 2);
        assert_eq!(
            reason.message,
            "dependency `a` skipped: not needed; dependency `b` failed: boom"
        );
    }

    fn empty_graph() -> Graph {
        Graph {
            nodes: BTreeMap::new(),
            order: vec![],
        }
    }

    fn state_map(left: NodeState, right: NodeState) -> BTreeMap<String, NodeState> {
        BTreeMap::from([("a".into(), left), ("b".into(), right)])
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ExpectedJoin {
        AllSatisfied,
        AnySatisfied,
        Waiting,
        AnyExhausted,
    }

    fn dependency_state_pairs() -> Vec<(
        &'static str,
        NodeState,
        &'static str,
        NodeState,
        ExpectedJoin,
    )> {
        let states = [
            ("pending", NodeState::Pending),
            ("running", NodeState::Running),
            ("success", NodeState::Success),
            ("skipped", NodeState::Skipped("skip".into())),
            ("failed", NodeState::Failed("fail".into())),
        ];
        let mut pairs = Vec::new();
        for (left_name, left) in &states {
            for (right_name, right) in &states {
                let expected = match (left, right) {
                    (NodeState::Success, NodeState::Success) => ExpectedJoin::AllSatisfied,
                    (NodeState::Success, _) | (_, NodeState::Success) => ExpectedJoin::AnySatisfied,
                    (
                        NodeState::Skipped(_) | NodeState::Failed(_),
                        NodeState::Skipped(_) | NodeState::Failed(_),
                    ) => ExpectedJoin::AnyExhausted,
                    _ => ExpectedJoin::Waiting,
                };
                pairs.push((
                    *left_name,
                    left.clone(),
                    *right_name,
                    right.clone(),
                    expected,
                ));
            }
        }
        pairs
    }

    macro_rules! join_all_case {
        ($name:ident, $left:expr, $right:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let graph = empty_graph();
                let node = join_node(OnDeps::AllSucceeded);
                let states = state_map($left, $right);
                assert_eq!(graph.needs_satisfied(&node, &states), $expected);
            }
        };
    }

    macro_rules! join_any_case {
        ($name:ident, $left:expr, $right:expr, $expected_ready:expr, $expected_skip:expr) => {
            #[test]
            fn $name() {
                let graph = empty_graph();
                let node = join_node(OnDeps::AnySucceeded);
                let states = state_map($left, $right);
                assert_eq!(graph.needs_satisfied(&node, &states), $expected_ready);
                assert_eq!(
                    graph.should_skip_by_dependencies(&node, &states).as_deref(),
                    $expected_skip
                );
            }
        };
    }

    join_all_case!(
        join_all_pending_pending_waits,
        NodeState::Pending,
        NodeState::Pending,
        false
    );
    join_all_case!(
        join_all_pending_running_waits,
        NodeState::Pending,
        NodeState::Running,
        false
    );
    join_all_case!(
        join_all_pending_success_waits,
        NodeState::Pending,
        NodeState::Success,
        false
    );
    join_all_case!(
        join_all_pending_skipped_waits,
        NodeState::Pending,
        NodeState::Skipped("skip".into()),
        false
    );
    join_all_case!(
        join_all_pending_failed_waits,
        NodeState::Pending,
        NodeState::Failed("fail".into()),
        false
    );
    join_all_case!(
        join_all_running_pending_waits,
        NodeState::Running,
        NodeState::Pending,
        false
    );
    join_all_case!(
        join_all_running_running_waits,
        NodeState::Running,
        NodeState::Running,
        false
    );
    join_all_case!(
        join_all_running_success_waits,
        NodeState::Running,
        NodeState::Success,
        false
    );
    join_all_case!(
        join_all_running_skipped_waits,
        NodeState::Running,
        NodeState::Skipped("skip".into()),
        false
    );
    join_all_case!(
        join_all_running_failed_waits,
        NodeState::Running,
        NodeState::Failed("fail".into()),
        false
    );
    join_all_case!(
        join_all_success_pending_waits,
        NodeState::Success,
        NodeState::Pending,
        false
    );
    join_all_case!(
        join_all_success_running_waits,
        NodeState::Success,
        NodeState::Running,
        false
    );
    join_all_case!(
        join_all_success_success_runs,
        NodeState::Success,
        NodeState::Success,
        true
    );
    join_all_case!(
        join_all_success_skipped_waits,
        NodeState::Success,
        NodeState::Skipped("skip".into()),
        false
    );
    join_all_case!(
        join_all_success_failed_waits,
        NodeState::Success,
        NodeState::Failed("fail".into()),
        false
    );
    join_all_case!(
        join_all_skipped_pending_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Pending,
        false
    );
    join_all_case!(
        join_all_skipped_running_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Running,
        false
    );
    join_all_case!(
        join_all_skipped_success_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Success,
        false
    );
    join_all_case!(
        join_all_skipped_skipped_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Skipped("skip".into()),
        false
    );
    join_all_case!(
        join_all_skipped_failed_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Failed("fail".into()),
        false
    );
    join_all_case!(
        join_all_failed_pending_waits,
        NodeState::Failed("fail".into()),
        NodeState::Pending,
        false
    );
    join_all_case!(
        join_all_failed_running_waits,
        NodeState::Failed("fail".into()),
        NodeState::Running,
        false
    );
    join_all_case!(
        join_all_failed_success_waits,
        NodeState::Failed("fail".into()),
        NodeState::Success,
        false
    );
    join_all_case!(
        join_all_failed_skipped_waits,
        NodeState::Failed("fail".into()),
        NodeState::Skipped("skip".into()),
        false
    );
    join_all_case!(
        join_all_failed_failed_waits,
        NodeState::Failed("fail".into()),
        NodeState::Failed("fail".into()),
        false
    );

    join_any_case!(
        join_any_pending_pending_waits,
        NodeState::Pending,
        NodeState::Pending,
        false,
        None
    );
    join_any_case!(
        join_any_pending_running_waits,
        NodeState::Pending,
        NodeState::Running,
        false,
        None
    );
    join_any_case!(
        join_any_pending_success_runs,
        NodeState::Pending,
        NodeState::Success,
        true,
        None
    );
    join_any_case!(
        join_any_pending_skipped_waits,
        NodeState::Pending,
        NodeState::Skipped("skip".into()),
        false,
        None
    );
    join_any_case!(
        join_any_pending_failed_waits,
        NodeState::Pending,
        NodeState::Failed("fail".into()),
        false,
        None
    );
    join_any_case!(
        join_any_running_pending_waits,
        NodeState::Running,
        NodeState::Pending,
        false,
        None
    );
    join_any_case!(
        join_any_running_running_waits,
        NodeState::Running,
        NodeState::Running,
        false,
        None
    );
    join_any_case!(
        join_any_running_success_runs,
        NodeState::Running,
        NodeState::Success,
        true,
        None
    );
    join_any_case!(
        join_any_running_skipped_waits,
        NodeState::Running,
        NodeState::Skipped("skip".into()),
        false,
        None
    );
    join_any_case!(
        join_any_running_failed_waits,
        NodeState::Running,
        NodeState::Failed("fail".into()),
        false,
        None
    );
    join_any_case!(
        join_any_success_pending_runs,
        NodeState::Success,
        NodeState::Pending,
        true,
        None
    );
    join_any_case!(
        join_any_success_running_runs,
        NodeState::Success,
        NodeState::Running,
        true,
        None
    );
    join_any_case!(
        join_any_success_success_runs,
        NodeState::Success,
        NodeState::Success,
        true,
        None
    );
    join_any_case!(
        join_any_success_skipped_runs,
        NodeState::Success,
        NodeState::Skipped("skip".into()),
        true,
        None
    );
    join_any_case!(
        join_any_success_failed_runs,
        NodeState::Success,
        NodeState::Failed("fail".into()),
        true,
        None
    );
    join_any_case!(
        join_any_skipped_pending_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Pending,
        false,
        None
    );
    join_any_case!(
        join_any_skipped_running_waits,
        NodeState::Skipped("skip".into()),
        NodeState::Running,
        false,
        None
    );
    join_any_case!(
        join_any_skipped_success_runs,
        NodeState::Skipped("skip".into()),
        NodeState::Success,
        true,
        None
    );
    join_any_case!(
        join_any_skipped_skipped_exhausts,
        NodeState::Skipped("skip".into()),
        NodeState::Skipped("skip".into()),
        false,
        Some("on_deps=any_succeeded had no successful dependency")
    );
    join_any_case!(
        join_any_skipped_failed_exhausts,
        NodeState::Skipped("skip".into()),
        NodeState::Failed("fail".into()),
        false,
        Some("on_deps=any_succeeded had no successful dependency")
    );
    join_any_case!(
        join_any_failed_pending_waits,
        NodeState::Failed("fail".into()),
        NodeState::Pending,
        false,
        None
    );
    join_any_case!(
        join_any_failed_running_waits,
        NodeState::Failed("fail".into()),
        NodeState::Running,
        false,
        None
    );
    join_any_case!(
        join_any_failed_success_runs,
        NodeState::Failed("fail".into()),
        NodeState::Success,
        true,
        None
    );
    join_any_case!(
        join_any_failed_skipped_exhausts,
        NodeState::Failed("fail".into()),
        NodeState::Skipped("skip".into()),
        false,
        Some("on_deps=any_succeeded had no successful dependency")
    );
    join_any_case!(
        join_any_failed_failed_exhausts,
        NodeState::Failed("fail".into()),
        NodeState::Failed("fail".into()),
        false,
        Some("on_deps=any_succeeded had no successful dependency")
    );

    fn join_node(on_deps: OnDeps) -> NodeDef {
        NodeDef {
            id: "c".into(),
            kind: StepType::from("write"),
            needs: vec!["a".into(), "b".into()],
            when: None,
            on_deps,
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            params: Default::default(),
        }
    }
}
