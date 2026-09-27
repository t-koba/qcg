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
                write_node("a", vec!["b".into()]),
                write_node("b", vec!["a".into()]),
            ],
            parallel: Vec::new(),
            blocks: Default::default(),
            outputs: OutputSpec::default(),
            failure: Default::default(),
            retention: Default::default(),
            audit: Default::default(),
            hooks: Default::default(),
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

    fn join_node(on_deps: OnDeps) -> NodeDef {
        NodeDef {
            on_deps,
            ..write_node("c", vec!["a".into(), "b".into()])
        }
    }

    /// A literal `write` node with the default node options, so a test states
    /// only the two fields it actually varies: the id and the `needs` edges.
    fn write_node(id: &str, needs: Vec<String>) -> NodeDef {
        NodeDef {
            id: id.into(),
            kind: StepType::literal("write"),
            needs,
            when: None,
            on_deps: Default::default(),
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

#[cfg(test)]
mod block_needs_tests {
    use super::*;
    use crate::manifest::Manifest;

    #[test]
    fn foreach_block_children_must_not_declare_needs() {
        // E10: block children run in declared order gated only by `when`;
        // a `needs` edge would be silently ignored, so refuse it
        // fail-closed instead of running a different order than stated.
        let manifest = toml::from_str::<Manifest>(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "loop"
type = "foreach"
[flow.params]
items = "inputs.items"
subflow = "item"
max_iterations = 4

[[blocks.item]]
id = "first"
type = "write"

[[blocks.item]]
id = "second"
needs = ["first"]
type = "write"
"#,
        )
        .expect("manifest should parse");
        let error = Graph::build(&manifest).expect_err("block needs must be refused");
        assert!(
            error.contains("must not declare `needs`"),
            "refusal must name the cause: {error}"
        );
    }
}
