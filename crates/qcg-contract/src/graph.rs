mod build;
mod refs;
mod types;

pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GeneratorMeta,
        manifest::{Manifest, NodeDef, OnDeps, OutputSpec, Permissions, StepType},
    };
    use std::collections::BTreeMap;

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
                    retry: None,
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
                    retry: None,
                    params: Default::default(),
                },
            ],
            parallel: Vec::new(),
            blocks: Default::default(),
            outputs: OutputSpec::default(),
            failure: Default::default(),
            journal: Default::default(),
            assets: Default::default(),
            dependencies: Default::default(),
        };
        assert!(
            Graph::build(&manifest)
                .unwrap_err()
                .contains("cycle detected")
        );
    }

    #[test]
    fn accepts_ask_user_repair_on_exhausted_strategy() {
        let manifest = toml::from_str::<Manifest>(
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
        .expect("ask_user is a valid exhaustion action");
        Graph::build(&manifest).expect("ask_user does not introduce a graph edge");
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

    #[test]
    fn join_all_needs_satisfied_truth_table() {
        // Consolidated scheduler truth table (was one test per
        // cell): every dependency-state pair pins whether the
        // join may run.
        let graph = empty_graph();
        let node = join_node(OnDeps::AllSucceeded);
        let cases = [
            (
                "join_all_pending_pending_waits",
                NodeState::Pending,
                NodeState::Pending,
                false,
            ),
            (
                "join_all_pending_running_waits",
                NodeState::Pending,
                NodeState::Running,
                false,
            ),
            (
                "join_all_pending_success_waits",
                NodeState::Pending,
                NodeState::Success,
                false,
            ),
            (
                "join_all_pending_skipped_waits",
                NodeState::Pending,
                NodeState::Skipped("skip".into()),
                false,
            ),
            (
                "join_all_pending_failed_waits",
                NodeState::Pending,
                NodeState::Failed("fail".into()),
                false,
            ),
            (
                "join_all_running_pending_waits",
                NodeState::Running,
                NodeState::Pending,
                false,
            ),
            (
                "join_all_running_running_waits",
                NodeState::Running,
                NodeState::Running,
                false,
            ),
            (
                "join_all_running_success_waits",
                NodeState::Running,
                NodeState::Success,
                false,
            ),
            (
                "join_all_running_skipped_waits",
                NodeState::Running,
                NodeState::Skipped("skip".into()),
                false,
            ),
            (
                "join_all_running_failed_waits",
                NodeState::Running,
                NodeState::Failed("fail".into()),
                false,
            ),
            (
                "join_all_success_pending_waits",
                NodeState::Success,
                NodeState::Pending,
                false,
            ),
            (
                "join_all_success_running_waits",
                NodeState::Success,
                NodeState::Running,
                false,
            ),
            (
                "join_all_success_success_runs",
                NodeState::Success,
                NodeState::Success,
                true,
            ),
            (
                "join_all_success_skipped_waits",
                NodeState::Success,
                NodeState::Skipped("skip".into()),
                false,
            ),
            (
                "join_all_success_failed_waits",
                NodeState::Success,
                NodeState::Failed("fail".into()),
                false,
            ),
            (
                "join_all_skipped_pending_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Pending,
                false,
            ),
            (
                "join_all_skipped_running_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Running,
                false,
            ),
            (
                "join_all_skipped_success_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Success,
                false,
            ),
            (
                "join_all_skipped_skipped_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Skipped("skip".into()),
                false,
            ),
            (
                "join_all_skipped_failed_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Failed("fail".into()),
                false,
            ),
            (
                "join_all_failed_pending_waits",
                NodeState::Failed("fail".into()),
                NodeState::Pending,
                false,
            ),
            (
                "join_all_failed_running_waits",
                NodeState::Failed("fail".into()),
                NodeState::Running,
                false,
            ),
            (
                "join_all_failed_success_waits",
                NodeState::Failed("fail".into()),
                NodeState::Success,
                false,
            ),
            (
                "join_all_failed_skipped_waits",
                NodeState::Failed("fail".into()),
                NodeState::Skipped("skip".into()),
                false,
            ),
            (
                "join_all_failed_failed_waits",
                NodeState::Failed("fail".into()),
                NodeState::Failed("fail".into()),
                false,
            ),
        ];
        // Every row runs: divergences collect under their original case
        // names and report together, so one failure never masks another.
        let mut diverged = Vec::new();
        for (name, left, right, expected) in cases {
            let states = state_map(left, right);
            if graph.needs_satisfied(&node, &states) != expected {
                diverged.push(name);
            }
        }
        assert!(
            diverged.is_empty(),
            "join_all cases diverged: {}",
            diverged.join(", ")
        );
    }

    #[test]
    fn join_any_ready_and_skip_truth_table() {
        // Consolidated scheduler truth table (was one test per
        // cell): every dependency-state pair pins readiness and
        // the terminal skip reason.
        let graph = empty_graph();
        let node = join_node(OnDeps::AnySucceeded);
        let cases = [
            (
                "join_any_pending_pending_waits",
                NodeState::Pending,
                NodeState::Pending,
                false,
                None,
            ),
            (
                "join_any_pending_running_waits",
                NodeState::Pending,
                NodeState::Running,
                false,
                None,
            ),
            (
                "join_any_pending_success_runs",
                NodeState::Pending,
                NodeState::Success,
                true,
                None,
            ),
            (
                "join_any_pending_skipped_waits",
                NodeState::Pending,
                NodeState::Skipped("skip".into()),
                false,
                None,
            ),
            (
                "join_any_pending_failed_waits",
                NodeState::Pending,
                NodeState::Failed("fail".into()),
                false,
                None,
            ),
            (
                "join_any_running_pending_waits",
                NodeState::Running,
                NodeState::Pending,
                false,
                None,
            ),
            (
                "join_any_running_running_waits",
                NodeState::Running,
                NodeState::Running,
                false,
                None,
            ),
            (
                "join_any_running_success_runs",
                NodeState::Running,
                NodeState::Success,
                true,
                None,
            ),
            (
                "join_any_running_skipped_waits",
                NodeState::Running,
                NodeState::Skipped("skip".into()),
                false,
                None,
            ),
            (
                "join_any_running_failed_waits",
                NodeState::Running,
                NodeState::Failed("fail".into()),
                false,
                None,
            ),
            (
                "join_any_success_pending_runs",
                NodeState::Success,
                NodeState::Pending,
                true,
                None,
            ),
            (
                "join_any_success_running_runs",
                NodeState::Success,
                NodeState::Running,
                true,
                None,
            ),
            (
                "join_any_success_success_runs",
                NodeState::Success,
                NodeState::Success,
                true,
                None,
            ),
            (
                "join_any_success_skipped_runs",
                NodeState::Success,
                NodeState::Skipped("skip".into()),
                true,
                None,
            ),
            (
                "join_any_success_failed_runs",
                NodeState::Success,
                NodeState::Failed("fail".into()),
                true,
                None,
            ),
            (
                "join_any_skipped_pending_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Pending,
                false,
                None,
            ),
            (
                "join_any_skipped_running_waits",
                NodeState::Skipped("skip".into()),
                NodeState::Running,
                false,
                None,
            ),
            (
                "join_any_skipped_success_runs",
                NodeState::Skipped("skip".into()),
                NodeState::Success,
                true,
                None,
            ),
            (
                "join_any_skipped_skipped_exhausts",
                NodeState::Skipped("skip".into()),
                NodeState::Skipped("skip".into()),
                false,
                Some("on_deps=any_succeeded had no successful dependency"),
            ),
            (
                "join_any_skipped_failed_exhausts",
                NodeState::Skipped("skip".into()),
                NodeState::Failed("fail".into()),
                false,
                Some("on_deps=any_succeeded had no successful dependency"),
            ),
            (
                "join_any_failed_pending_waits",
                NodeState::Failed("fail".into()),
                NodeState::Pending,
                false,
                None,
            ),
            (
                "join_any_failed_running_waits",
                NodeState::Failed("fail".into()),
                NodeState::Running,
                false,
                None,
            ),
            (
                "join_any_failed_success_runs",
                NodeState::Failed("fail".into()),
                NodeState::Success,
                true,
                None,
            ),
            (
                "join_any_failed_skipped_exhausts",
                NodeState::Failed("fail".into()),
                NodeState::Skipped("skip".into()),
                false,
                Some("on_deps=any_succeeded had no successful dependency"),
            ),
            (
                "join_any_failed_failed_exhausts",
                NodeState::Failed("fail".into()),
                NodeState::Failed("fail".into()),
                false,
                Some("on_deps=any_succeeded had no successful dependency"),
            ),
        ];
        // Every row runs: divergences collect under their original case
        // names and report together, so one failure never masks another.
        let mut diverged = Vec::new();
        for (name, left, right, expected_ready, expected_skip) in cases {
            let states = state_map(left, right);
            if graph.needs_satisfied(&node, &states) != expected_ready
                || graph.should_skip_by_dependencies(&node, &states).as_deref() != expected_skip
            {
                diverged.push(name);
            }
        }
        assert!(
            diverged.is_empty(),
            "join_any cases diverged: {}",
            diverged.join(", ")
        );
    }

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
            retry: None,
            params: Default::default(),
        }
    }
}
