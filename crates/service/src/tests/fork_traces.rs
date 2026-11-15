use super::support::{TempGuard, temp_run_dir, test_service, wait_for_terminal_snapshot};
use crate::{read_journal_events, run_meta_dir};
use api::ForkStatePatch;
use camino::{Utf8Path, Utf8PathBuf};
use contract::ReadIdentity;
use engine::{JournalLimits, RunState};
use serde_json::{Value, json};

const TRACES: &str = ".dependency-traces.jsonl";

async fn source(root: &Utf8Path) -> Utf8PathBuf {
    let package = root.join("generators/trace-test");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "trace-test"
name = "Trace fork test"
version = "0.1.0"
[[flow]]
id = "render"
type = "render"
[flow.params]
template = "data.j2"
output_file = "data.json"
[[flow]]
id = "node"
type = "check.format"
needs = ["render"]
[flow.params]
source = "data.json"
content = "json"
[[flow]]
id = "later"
type = "check.format"
needs = ["node"]
[flow.params]
source = "data.json"
content = "json"
[permissions]
fs_read = ["workspace"]
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
"#,
    )
    .unwrap();
    std::fs::write(package.join("data.j2"), "{}").unwrap();
    let service = test_service(vec![root.join("generators")], root.join("runs"));
    let id = service
        .start_run(api::StartRun {
            generator_id: "trace-test".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        api::RunStatus::Succeeded
    );
    service.shutdown_active_runs().await.unwrap();
    root.join("runs").join(id)
}

fn cutoff(source: &Utf8Path) -> u64 {
    read_journal_events(source)
        .unwrap()
        .iter()
        .find(|event| event["t"] == "step_finished" && event["node"] == "node")
        .unwrap()["seq"]
        .as_u64()
        .unwrap()
}

fn fork(source: &Utf8Path, target: &Utf8Path) {
    crate::run_dirs::prepare_checkpoint_fork(
        source,
        "source",
        target,
        "child",
        cutoff(source),
        &ForkStatePatch::default(),
    )
    .unwrap();
    let state = RunState::fold_journal(&run_meta_dir(target).join("journal.jsonl")).unwrap();
    assert!(matches!(
        state.nodes.get(&model::NodePath::root("node")),
        Some(engine::NodeOutcome::Success { .. })
    ));
    assert!(!state.nodes.contains_key(&model::NodePath::root("later")));
    assert_eq!(
        read_journal_events(target).unwrap().last().unwrap()["t"],
        "run_forked"
    );
}

fn records(dir: &Utf8Path) -> Vec<Value> {
    engine::read_journal_values(&run_meta_dir(dir).join(TRACES), JournalLimits::default())
        .unwrap()
        .events
}

#[tokio::test]
async fn fork_traces_follow_journal_not_audit_ties_and_exclude_later_records() {
    let root = temp_run_dir("fork-traces-ties");
    let _guard = TempGuard(root.clone());
    let source = source(&root).await;
    // An earlier audit tie shifts the durable outcome by one child sequence.
    // An identical durable-looking event in audit must not consume its record.
    let events = read_journal_events(&source).unwrap();
    let source_outcome = events
        .iter()
        .find(|event| event["t"] == "step_finished" && event["node"] == "node")
        .unwrap();
    let audit = [
        json!({"t":"llm_chunk", "seq":1, "run_id":"source", "text":"audit"}),
        source_outcome.clone(),
    ];
    std::fs::write(
        run_meta_dir(&source).join("audit.jsonl"),
        format!("{}\n{}\n", audit[0], audit[1]),
    )
    .unwrap();
    let target = root.join("child");
    fork(&source, &target);
    let target_events = read_journal_events(&target).unwrap();
    let outcome = target_events
        .iter()
        .find(|event| event["t"] == "step_finished" && event["node"] == "node")
        .unwrap();
    let target_seq = outcome["seq"].as_u64().unwrap();
    let source_position = events
        .iter()
        .position(|event| event == source_outcome)
        .unwrap();
    assert_eq!(target_seq, source_position as u64 + 2);
    assert_eq!(outcome["run_id"], "child");
    assert_eq!(outcome["trace_id"], api::trace_id_for_run("child"));
    assert_eq!(outcome["span_id"], api::span_id_for_seq(target_seq));
    assert!(source_outcome.get("parent_span_id").is_some());
    assert_eq!(
        outcome["parent_span_id"],
        api::span_id_for_scope("child", "step:node")
    );
    let traces = records(&target);
    assert!(traces.iter().all(|record| record["owner"] != "later"));
    let trace = traces
        .iter()
        .find(|record| record["owner"] == "node")
        .unwrap();
    assert_eq!(trace["seq"], target_seq);
    let ReadIdentity::Digest(digest) = ReadIdentity::of(Some(outcome)) else {
        panic!("digest")
    };
    assert_eq!(trace["event_digest"], json!(digest));
    assert_ne!(
        trace["record_digest"],
        records(&source)
            .iter()
            .find(|record| record["owner"] == "node")
            .unwrap()["record_digest"]
    );
    let target_audit = engine::read_journal_values(
        &run_meta_dir(&target).join("audit.jsonl"),
        JournalLimits::default(),
    )
    .unwrap()
    .events;
    assert_eq!(target_audit[1]["seq"], target_seq + 1);
    let cache = std::fs::read_to_string(run_meta_dir(&target).join("state.json")).unwrap();
    assert!(!cache.contains("private-node"));
    assert!(!cache.contains("event_digest"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(run_meta_dir(&target).join(TRACES))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
    }
}

#[tokio::test]
async fn fork_traces_unavailable_evidence_never_changes_ordinary_fork() {
    for mode in [
        "missing",
        "corrupt",
        "duplicate",
        "incomplete",
        "audit_only",
        "ambiguous",
        "write_failure",
        "permissive",
    ] {
        let root = temp_run_dir(&format!("fork-traces-{mode}"));
        let _guard = TempGuard(root.clone());
        let source = source(&root).await;
        let cutoff = cutoff(&source);
        let trace_path = run_meta_dir(&source).join(TRACES);
        match mode {
            "missing" => std::fs::remove_file(&trace_path).unwrap(),
            "corrupt" => std::fs::write(&trace_path, b"bad-json\n").unwrap(),
            "duplicate" => {
                let original = std::fs::read_to_string(&trace_path).unwrap();
                std::fs::write(&trace_path, format!("{original}{original}")).unwrap();
            }
            "incomplete" => {
                let mut traces = records(&source);
                traces[0]["complete"] = json!(false);
                std::fs::write(
                    &trace_path,
                    traces
                        .iter()
                        .map(|trace| format!("{trace}\n"))
                        .collect::<String>(),
                )
                .unwrap();
            }
            "audit_only" | "ambiguous" => {
                let mut events = read_journal_events(&source).unwrap();
                let index = events
                    .iter()
                    .position(|event| event["seq"] == cutoff)
                    .unwrap();
                let event = events[index].clone();
                if mode == "audit_only" {
                    std::fs::write(
                        run_meta_dir(&source).join("audit.jsonl"),
                        format!("{event}\n"),
                    )
                    .unwrap();
                    events[index] = json!({"t":"audit_degraded", "seq":cutoff, "run_id":"source", "ts":"2026-01-01T00:00:00Z"});
                } else {
                    events.push(event);
                }
                let journal = events
                    .iter()
                    .map(|event| format!("{event}\n"))
                    .collect::<String>();
                std::fs::write(run_meta_dir(&source).join("journal.jsonl"), journal).unwrap();
            }
            "permissive" => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    std::fs::set_permissions(&trace_path, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                #[cfg(not(unix))]
                continue;
            }
            "write_failure" => {}
            _ => unreachable!(),
        }
        let target = root.join("child");
        if mode == "write_failure" {
            // Existing private-path obstruction is an optional publication
            // failure, not an ordinary target journal/workspace failure.
            std::fs::create_dir_all(run_meta_dir(&target).join(TRACES)).unwrap();
        }
        crate::run_dirs::prepare_checkpoint_fork(
            &source,
            "source",
            &target,
            "child",
            cutoff,
            &ForkStatePatch::default(),
        )
        .unwrap();
        assert!(!run_meta_dir(&target).join(TRACES).is_file(), "{mode}");
        let state = RunState::fold_journal(&run_meta_dir(&target).join("journal.jsonl")).unwrap();
        if mode != "audit_only" {
            assert!(
                matches!(
                    state.nodes.get(&model::NodePath::root("node")),
                    Some(engine::NodeOutcome::Success { .. })
                ),
                "{mode}"
            );
        }
        assert_eq!(
            read_journal_events(&target).unwrap().last().unwrap()["t"],
            "run_forked"
        );
        assert!(
            std::fs::read_dir(run_meta_dir(&target))
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".part-"))
        );
    }
}
