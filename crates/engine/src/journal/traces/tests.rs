use super::*;
use crate::{JournalWriter, NodeOutcome, RunState, read_journal_values};
use camino::Utf8PathBuf;
use serde_json::json;

struct Fixture {
    path: Utf8PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("qcg-dependencies-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        Self {
            path: Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap(),
        }
    }
    fn writer(&self) -> JournalWriter {
        JournalWriter::create(&self.path, "dependencies", false, None).unwrap()
    }
    fn fold(&self) -> RunState {
        RunState::fold_journal(&self.path).unwrap()
    }
    fn records(&self) -> Vec<Value> {
        read_journal_values(
            &self.path.with_file_name(FILE_NAME),
            JournalLimits::default(),
        )
        .unwrap()
        .events
    }
    fn replace(&self, records: &[Value]) {
        let mut text = String::new();
        for record in records {
            text.push_str(&format!("{record}\n"));
        }
        std::fs::write(self.path.with_file_name(FILE_NAME), text).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.path.parent().unwrap()).unwrap();
    }
}

fn reads(owner: &str) -> ReadRecorder {
    let recorder = ReadRecorder::new(policy::DEFAULT_MAX_JOURNAL_EVENT_BYTES)
        .for_owner(ReadOwner::Node(owner.into()));
    recorder.record(
        &["inputs".into(), format!("private-{owner}")],
        ReadKind::Lookup,
        ReadIdentity::of(Some(&json!("never-store-this-value"))),
    );
    recorder
}
fn finish(writer: &JournalWriter, owner: &str, status: &str) {
    writer
        .event("step_finished", json!({"node": owner, "status": status}))
        .unwrap();
}
fn assert_success(state: &RunState, owner: &str) {
    assert!(matches!(
        state.nodes.get(&model::NodePath::root(owner)),
        Some(NodeOutcome::Success { .. })
    ));
}
fn assert_trace(state: &RunState, owner: &str, seq: u64) {
    let evidence = &state.dependency_traces.owners[owner];
    assert_eq!(evidence.seq, seq);
    assert!(evidence.trace.complete_for(&ReadOwner::Node(owner.into())));
    assert_eq!(evidence.trace.reads.len(), 1);
    assert!(
        evidence
            .trace
            .reads
            .iter()
            .all(|read| read.owner == ReadOwner::Node(owner.into())
                && read.path == ["inputs".to_string(), format!("private-{owner}")])
    );
}

#[test]
fn durable_restart_cache_and_public_output_are_separate() {
    let f = Fixture::new();
    let (sender, mut receiver) = tokio::sync::broadcast::channel(8);
    let writer = JournalWriter::create(&f.path, "dependencies", false, Some(sender))
        .unwrap()
        .with_read_recorder(reads("node"))
        .unwrap();
    finish(&writer, "node", "success");
    assert_trace(&writer.state(), "node", 1);
    let public = std::fs::read_to_string(&f.path).unwrap();
    let api = serde_json::to_string(&receiver.try_recv().unwrap()).unwrap();
    assert!(receiver.try_recv().is_err());
    let cache = std::fs::read_to_string(f.path.with_file_name("state.json")).unwrap();
    for output in [&public, &api, &cache] {
        for private in [
            "private-node",
            "never-store-this-value",
            "dependency_traces",
            "event_digest",
            "reads",
        ] {
            assert!(!output.contains(private), "leaked {private}");
        }
    }
    assert!(
        !std::fs::read_to_string(f.path.with_file_name(FILE_NAME))
            .unwrap()
            .contains("never-store-this-value")
    );
    let event = read_journal_values(&f.path, JournalLimits::default())
        .unwrap()
        .events
        .remove(0);
    assert_eq!(
        f.records()[0]["event_digest"],
        json!(digest(&event).unwrap())
    );
    assert_trace(&f.fold(), "node", 1);
    assert_trace(&f.writer().state(), "node", 1);
    // Canonical event binding is independent of public object key order.
    let reversed = event
        .as_object()
        .unwrap()
        .iter()
        .rev()
        .map(|(key, value)| format!("{key:?}:{value}"))
        .collect::<Vec<_>>()
        .join(",");
    std::fs::write(&f.path, format!("{{{reversed}}}\n")).unwrap();
    assert_trace(&f.fold(), "node", 1);
    // Plain cache deserialization and value-only folds cannot grant authority.
    let cached: RunState = serde_json::from_str(&cache).unwrap();
    assert!(cached.dependency_traces.owners.is_empty());
    assert!(
        RunState::fold_values(&[event])
            .unwrap()
            .dependency_traces
            .owners
            .is_empty()
    );
    // A present, up-to-date state cache must not hide a missing private stream.
    std::fs::remove_file(f.path.with_file_name(FILE_NAME)).unwrap();
    let mut forged_cache: Value = serde_json::from_str(&cache).unwrap();
    forged_cache["dependency_traces"] = json!({"owners":{"node":{"complete":true}}});
    std::fs::write(
        f.path.with_file_name("state.json"),
        forged_cache.to_string(),
    )
    .unwrap();
    let restarted = f.writer().state();
    assert_success(&restarted, "node");
    assert!(restarted.dependency_traces.owners.is_empty());
}

#[test]
fn exact_owner_seq_digest_version_and_supported_identity_are_required() {
    let f = Fixture::new();
    let writer = f.writer().with_read_recorder(reads("a")).unwrap();
    finish(&writer, "a", "success");
    let original = f.records();
    for (field, bad) in [
        ("owner", json!("a/child")),
        ("seq", json!(2)),
        ("event_digest", json!(vec![0_u8; 32])),
        ("version", json!(0)),
        ("version", json!(VERSION + 1)),
        ("complete", json!(false)),
    ] {
        let mut records = original.clone();
        records[0][field] = bad;
        f.replace(&records);
        let state = f.fold();
        assert_success(&state, "a");
        assert!(
            state.dependency_traces.owners.is_empty(),
            "accepted {field}"
        );
    }
    let mut records = original.clone();
    records[0]["reads"][0]["path"][1] = json!("different-but-valid-path");
    f.replace(&records);
    assert!(f.fold().dependency_traces.owners.is_empty());
    let mut records = original.clone();
    records[0]["reads"][0]["identity"] = json!("unsupported");
    f.replace(&records);
    assert!(f.fold().dependency_traces.owners.is_empty());
    f.replace(&[original[0].clone(), original[0].clone()]);
    assert!(f.fold().dependency_traces.owners.is_empty());
    f.replace(&original);
    // Alter a durable event without altering seq or owner: old trace cannot bind.
    let mut event = read_journal_values(&f.path, JournalLimits::default())
        .unwrap()
        .events
        .remove(0);
    event["status"] = json!("failed");
    std::fs::write(&f.path, format!("{event}\n")).unwrap();
    let state = f.fold();
    assert!(matches!(
        state.nodes.get(&model::NodePath::root("a")),
        Some(NodeOutcome::Failed { .. })
    ));
    assert!(state.dependency_traces.owners.is_empty());
}

#[test]
fn corruption_legacy_and_private_limits_never_break_ordinary_replay() {
    let f = Fixture::new();
    let writer = f.writer().with_read_recorder(reads("a")).unwrap();
    finish(&writer, "a", "success");
    let original = f.records();
    for text in ["{bad}\n", "{\"version\":1", "{}\n", "{\"reads\":[]}\n"] {
        std::fs::write(f.path.with_file_name(FILE_NAME), text).unwrap();
        let state = f.fold();
        assert_success(&state, "a");
        assert!(state.dependency_traces.owners.is_empty());
        assert_success(&f.writer().state(), "a");
    }
    f.replace(&original);
    // Private lines are larger than the minimal public outcome. The same
    // configured line/total/count bounds apply independently to each stream.
    let public_bytes = std::fs::metadata(&f.path).unwrap().len() as usize;
    let private_bytes = std::fs::metadata(f.path.with_file_name(FILE_NAME))
        .unwrap()
        .len() as usize;
    assert!(private_bytes > public_bytes);
    for limits in [
        JournalLimits {
            max_event_bytes: Some(public_bytes),
            ..JournalLimits::default()
        },
        JournalLimits {
            max_total_bytes: Some(public_bytes),
            ..JournalLimits::default()
        },
    ] {
        let state = RunState::fold_journal_with_limits(&f.path, limits).unwrap();
        assert_success(&state, "a");
        assert!(state.dependency_traces.owners.is_empty());
    }
    let mut extra = original[0].clone();
    extra["seq"] = json!(2);
    f.replace(&[original[0].clone(), extra]);
    let state = RunState::fold_journal_with_limits(
        &f.path,
        JournalLimits {
            max_event_count: Some(1),
            ..JournalLimits::default()
        },
    )
    .unwrap();
    assert_success(&state, "a");
    assert!(state.dependency_traces.owners.is_empty());
    f.replace(&original);
    let mut state = f.fold();
    let events = read_journal_values(&f.path, JournalLimits::default())
        .unwrap()
        .events;
    TraceJournal::restore(
        &mut state,
        &events,
        &f.path,
        JournalLimits {
            max_state_bytes: Some(1),
            ..JournalLimits::default()
        },
    );
    assert_success(&state, "a");
    assert!(state.dependency_traces.owners.is_empty());
    f.replace(&[]);
    assert!(f.fold().dependency_traces.owners.is_empty());
}

#[test]
fn capture_overflow_unsupported_and_io_failure_do_not_change_outcomes() {
    for (mode, status) in ["success", "failed"].into_iter().flat_map(|status| {
        ["overflow", "unsupported", "io", "wire_limit"].map(|mode| (mode, status))
    }) {
        let f = Fixture::new();
        let recorder = reads("a");
        match mode {
            "overflow" => recorder.set_byte_limit(1),
            "unsupported" => recorder.unsupported(),
            "io" => std::fs::create_dir(f.path.with_file_name(FILE_NAME)).unwrap(),
            _ => {}
        }
        let writer = f.writer().with_read_recorder(recorder).unwrap();
        let writer = if mode == "wire_limit" {
            // Event fits, but its private record does not.
            let limits = JournalLimits {
                max_event_bytes: Some(400),
                ..JournalLimits::default()
            };
            let large = reads("a");
            large.record(
                &["private-only-limit".repeat(32)],
                ReadKind::Lookup,
                ReadIdentity::Missing,
            );
            JournalWriter::create_with_limits(&f.path, "dependencies", false, None, limits)
                .unwrap()
                .with_read_recorder(large)
                .unwrap()
        } else {
            writer
        };
        finish(&writer, "a", status);
        assert_eq!(
            read_journal_values(&f.path, JournalLimits::default())
                .unwrap()
                .events
                .len(),
            1
        );
        assert_eq!(
            matches!(
                writer.state().nodes[&model::NodePath::root("a")],
                NodeOutcome::Success { .. }
            ),
            status == "success"
        );
        assert!(writer.state().dependency_traces.owners.is_empty(), "{mode}");
        let state = f.fold();
        assert_eq!(
            matches!(
                state.nodes[&model::NodePath::root("a")],
                NodeOutcome::Success { .. }
            ),
            status == "success"
        );
        assert!(state.dependency_traces.owners.is_empty(), "{mode}");
    }
}

#[test]
fn replay_recharges_dependencies_and_rejects_memory_overflow() {
    let f = Fixture::new();
    finish(
        &f.writer().with_read_recorder(reads("a")).unwrap(),
        "a",
        "success",
    );
    let mut record = f.records().remove(0);
    record["reads"] = json!(
        (0..40)
            .map(|n| json!({"path":[n.to_string()],
        "kind":"lookup", "identity":"missing"}))
            .collect::<Vec<_>>()
    );
    let wire_bytes = serde_json::to_vec(&record).unwrap().len();
    f.replace(&[record]);
    let state = RunState::fold_journal_with_limits(
        &f.path,
        JournalLimits {
            max_event_bytes: Some(wire_bytes),
            ..JournalLimits::default()
        },
    )
    .unwrap();
    assert_success(&state, "a");
    assert!(state.dependency_traces.owners.is_empty());
}

#[test]
fn latest_outcome_missing_evidence_and_starts_cannot_reuse_old_trace() {
    let f = Fixture::new();
    let writer = f.writer().with_read_recorder(reads("a")).unwrap();
    finish(&writer, "a", "success");
    writer
        .event(
            "step_started",
            json!({"node":"a", "type":"test", "attempt":2}),
        )
        .unwrap();
    assert!(f.fold().dependency_traces.owners.is_empty());
    finish(&f.writer(), "a", "failed");
    assert!(f.fold().dependency_traces.owners.is_empty());
    // Bounded historical folds bind to the earlier actual outcome, not the tail.
    let state = RunState::fold_journal_through(&f.path, Some(1)).unwrap();
    assert_trace(&state, "a", 1);
}

#[test]
fn concurrent_owners_and_foreach_iteration_bindings_are_not_lost_or_duplicated() {
    let f = Fixture::new();
    let writer = f.writer();
    std::thread::scope(|scope| {
        for owner in ["each[0]/child", "each[1]/child"] {
            let writer = writer
                .clone_for_parallel()
                .unwrap()
                .with_read_recorder(reads(owner))
                .unwrap();
            scope.spawn(move || finish(&writer, owner, "success"));
        }
    });
    let recorder = reads("each[0]");
    recorder.merge(&reads("each[1]"));
    let writer = writer.with_read_recorder(recorder).unwrap();
    for index in [0, 1] {
        writer
            .event(
                "foreach_iteration_settled",
                json!({"node":"each", "index":index,
            "iteration":format!("each[{index}]"), "owner":format!("each[{index}]/child"),
            "classification":"completed_success", "drain_cause":"completed", "files":[]}),
            )
            .unwrap();
    }
    let records = f.records();
    assert_eq!(records.len(), 4);
    let events = read_journal_values(&f.path, JournalLimits::default())
        .unwrap()
        .events;
    let restarted = f.writer().state();
    for record in records {
        let seq = record["seq"].as_u64().unwrap();
        let event = &events[seq as usize - 1];
        let owner = record["owner"].as_str().unwrap();
        assert_eq!(trace_owner(event).as_deref(), Some(owner));
        assert_trace(&restarted, owner, seq);
    }
    // A second process/writer must resync private evidence even with a cache.
    let stale = f.writer();
    finish(
        &writer.with_read_recorder(reads("later")).unwrap(),
        "later",
        "success",
    );
    finish(
        &stale.with_read_recorder(reads("last")).unwrap(),
        "last",
        "success",
    );
    assert_eq!(f.fold().dependency_traces.owners.len(), 6);
    assert_eq!(stale.state().dependency_traces.owners.len(), 6);
    // Cache adoption after evidence disappears must not retain peer traces.
    std::fs::remove_file(f.path.with_file_name(FILE_NAME)).unwrap();
    finish(&f.writer(), "no-evidence", "success");
    finish(
        &stale.with_read_recorder(reads("after-loss")).unwrap(),
        "after-loss",
        "success",
    );
    assert_success(&stale.state(), "after-loss");
    assert!(stale.state().dependency_traces.owners.is_empty());
}

#[cfg(unix)]
#[test]
fn permissive_trace_files_are_ignored_without_changing_run_outcomes() {
    use std::os::unix::fs::PermissionsExt as _;

    let f = Fixture::new();
    let writer = f.writer().with_read_recorder(reads("private")).unwrap();
    finish(&writer, "private", "success");
    drop(writer);

    let trace = f.path.with_file_name(FILE_NAME);
    let mut permissions = std::fs::metadata(&trace).unwrap().permissions();
    permissions.set_mode(0o644);
    std::fs::set_permissions(&trace, permissions).unwrap();

    let state = f.fold();
    assert_success(&state, "private");
    assert!(state.dependency_traces.owners.is_empty());
    assert!(f.writer().state().dependency_traces.owners.is_empty());
}

#[cfg(unix)]
#[test]
fn private_evidence_cannot_alias_the_public_journal() {
    for symlink in [true, false] {
        let f = Fixture::new();
        let writer = f.writer().with_read_recorder(reads("a")).unwrap();
        let trace = f.path.with_file_name(FILE_NAME);
        if symlink {
            std::os::unix::fs::symlink(&f.path, &trace).unwrap();
        } else {
            std::fs::hard_link(&f.path, &trace).unwrap();
        }
        finish(&writer, "a", "success");
        let scan = read_journal_values(&f.path, JournalLimits::default()).unwrap();
        assert_eq!(scan.events.len(), 1);
        assert_eq!(scan.events[0]["t"], "step_finished");
        assert_success(&f.fold(), "a");
        assert!(f.fold().dependency_traces.owners.is_empty());
        assert!(f.writer().state().dependency_traces.owners.is_empty());
    }
}

fn number_history(events: &mut [Value]) {
    for (index, event) in events.iter_mut().enumerate() {
        event["seq"] = json!(index + 1);
        event["ts"] = json!("2026-01-01T00:00:00Z");
        event["run_id"] = json!("dependencies");
        event["trace_id"] = json!(api::trace_id_for_run("dependencies"));
        event["span_id"] = json!(api::span_id_for_seq((index + 1) as u64));
    }
}

fn failed_history() -> Vec<Value> {
    let mut events = vec![
        json!({"t":"run_started", "generator":"test", "generator_path":"test",
            "contract_sha256":"test", "inputs":{},
            "schema_version":api::JOURNAL_SCHEMA_VERSION, "elapsed_budget_mode":"wall_clock"}),
        json!({"t":"graph_resolved", "nodes":["a", "b"]}),
        json!({"t":"step_started", "node":"a", "type":"check.format", "attempt":1}),
        json!({"t":"budget_charged", "node":"a", "amount":1}),
        json!({"t":"step_finished", "node":"a", "status":"success"}),
        json!({"t":"step_started", "node":"b", "type":"check.format", "attempt":1, "parallel":false}),
        json!({"t":"budget_charged", "node":"b", "amount":1}),
        json!({"t":"step_finished", "node":"b", "status":"failed", "parallel":false}),
        json!({"t":"elapsed_accounted", "active_nanoseconds":1, "active_started_at":null}),
        // Summary counters deliberately do not prove execution or dependencies.
        json!({"t":"run_finished", "status":"failed", "metrics":model::RunMetrics::default()}),
    ];
    number_history(&mut events);
    events
}

fn retain_history(f: &Fixture, events: &[Value]) {
    let text = events
        .iter()
        .map(|event| format!("{event}\n"))
        .collect::<String>();
    std::fs::write(&f.path, text).unwrap();
    let records = events
        .iter()
        .filter_map(|event| {
            let owner = trace_owner(event)?;
            Record::capture(event, Some(&reads(&owner)))
                .map(|record| serde_json::to_value(record).unwrap())
        })
        .collect::<Vec<_>>();
    f.replace(&records);
    crate::journal::writer::write_clean_shutdown_marker(&f.path).unwrap();
}

#[test]
fn failed_restore_requires_history_without_changing_public_replay() {
    for queued in [false, true] {
        for run_error in [false, true] {
            let f = Fixture::new();
            let mut events = failed_history();
            if queued {
                let mut admission = events[0].clone();
                admission["t"] = json!("run_queued");
                events.insert(0, admission);
            }
            if run_error {
                *events.last_mut().unwrap() = json!({"t":"run_error", "error":"failed"});
            }
            number_history(&mut events);
            retain_history(&f, &events);
            let state = f.fold();
            assert_eq!(state.terminal, Some(crate::TerminalState::Failed));
            assert_trace(&state, "a", 5 + u64::from(queued));
            assert_trace(&state, "b", 8 + u64::from(queued));
            assert_trace(&f.writer().state(), "b", 8 + u64::from(queued));
        }
    }
    let f = Fixture::new();
    let mut events = failed_history();
    events.insert(
        3,
        json!({"t":"step_retry", "node":"a", "attempt":2,
        "max_attempts":2, "error":"retry"}),
    );
    number_history(&mut events);
    retain_history(&f, &events);
    let before = std::fs::read(&f.path).unwrap();
    let expected = serde_json::to_value(RunState::fold_values(&events).unwrap()).unwrap();
    for state in [f.fold(), f.writer().state()] {
        assert_eq!(serde_json::to_value(&state).unwrap(), expected);
        assert!(state.dependency_traces.owners.is_empty());
    }
    assert_eq!(std::fs::read(&f.path).unwrap(), before);
}

#[test]
fn failed_history_requires_clean_marker_and_newline_terminated_journal() {
    let f = Fixture::new();
    let events = failed_history();
    retain_history(&f, &events);
    assert_eq!(f.fold().dependency_traces.owners.len(), 2);

    let mut bytes = std::fs::read(&f.path).unwrap();
    assert_eq!(bytes.pop(), Some(b'\n'));
    std::fs::write(&f.path, &bytes).unwrap();
    let unterminated = f.fold();
    assert_eq!(unterminated.terminal, Some(crate::TerminalState::Failed));
    assert!(unterminated.dependency_traces.owners.is_empty());
    bytes.push(b'\n');
    std::fs::write(&f.path, bytes).unwrap();

    let marker = crate::journal::writer::clean_shutdown_marker_path(&f.path);
    std::fs::remove_file(&marker).unwrap();
    let without_marker = f.fold();
    assert_eq!(without_marker.terminal, Some(crate::TerminalState::Failed));
    assert!(without_marker.dependency_traces.owners.is_empty());

    // Startup repair may complete valid JSON lacking its final newline. The
    // missing terminal marker still prevents that repair from granting trace evidence.
    let mut bytes = std::fs::read(&f.path).unwrap();
    assert_eq!(bytes.pop(), Some(b'\n'));
    std::fs::write(&f.path, bytes).unwrap();
    let repaired = f.writer().state();
    assert_eq!(repaired.terminal, Some(crate::TerminalState::Failed));
    assert!(repaired.dependency_traces.owners.is_empty());
}

#[test]
fn canceled_and_interrupted_runs_never_retain_continuation_traces() {
    for (kind, code) in [
        ("run_canceled", "canceled"),
        ("run_interrupted", "interrupted"),
    ] {
        let f = Fixture::new();
        let mut events = failed_history();
        *events.last_mut().unwrap() = json!({
            "t":kind,
            "reason":{"code":code, "message":"test"}
        });
        number_history(&mut events);
        retain_history(&f, &events);
        let state = f.fold();
        let terminal = if kind == "run_canceled" {
            crate::TerminalState::Canceled
        } else {
            crate::TerminalState::Interrupted
        };
        assert_eq!(state.terminal, Some(terminal));
        assert!(state.dependency_traces.owners.is_empty());
    }
}

#[test]
fn failed_history_is_a_positive_allowlist_with_serial_settlement() {
    let limits = JournalLimits::default();
    let traces = OutcomeTraces::default();
    assert!(history::proves_failed_history(&failed_history(), &traces, limits).is_some());
    // No special event vocabulary may silently become evidence. Some payloads
    // are intentionally opaque: unrecognized/malformed records also fail shut.
    for kind in [
        "step_retry",
        "step_skipped",
        "step_interrupted",
        "step_replayed",
        "hook_failed",
        "unknown_future_kind",
        "side_effect",
        "effect_intent",
        "operation_started",
        "tool_call",
        "mcp_input_pending",
        "llm_call",
        "confirm_request",
        "user_interaction",
        "run_waiting",
        "foreach_iteration",
        "parallel_task_joined",
        "repair_attempt_started",
        "regenerate_attempt_started",
        "run_resumed",
        "run_forked",
        "run_queued",
        "run_started",
        "run_canceled",
        "run_interrupted",
        "run_finished",
        "run_error",
    ] {
        let mut events = failed_history();
        events.insert(6, json!({"t":kind, "node":"b"}));
        number_history(&mut events);
        assert!(
            history::proves_failed_history(&events, &traces, limits).is_none(),
            "{kind}"
        );
    }
    // This is a lifecycle proof, not executor authorization. A step type string
    // alone does not establish read-only behavior; continuation must separately
    // consult the current executor/contract safety gate before reusing any node.
    for step_type in ["llm.generate", "command", "custom.unknown"] {
        let mut events = failed_history();
        events[2]["type"] = json!(step_type);
        number_history(&mut events);
        assert!(
            history::proves_failed_history(&events, &traces, limits).is_some(),
            "executor classification is outside the lifecycle proof: {step_type}"
        );
    }
    for (index, field, value) in [
        (0, "schema_version", json!(1)),
        (0, "parent_run_id", json!("parent")),
        (0, "seq", json!(0)),
        (2, "attempt", json!(2)),
        (2, "parallel", json!(true)),
        (2, "node", json!("hook.before")),
        (2, "node", json!("each[0]/child")),
        (2, "node", json!("a@repair.check")),
        (2, "type", json!("foreach")),
        (3, "amount", json!(0)),
        (3, "amount", json!(2)),
        (3, "node", json!("b")),
        (4, "node", json!("b")),
        (4, "parallel", json!(true)),
        (4, "status", json!("regenerated")),
        (7, "status", json!("needs_user")),
        (7, "status", json!("success")),
        (9, "status", json!("success")),
        (4, "seq", json!(4)),
        (7, "run_id", json!("foreign")),
    ] {
        let mut events = failed_history();
        events[index][field] = value;
        assert!(
            history::proves_failed_history(&events, &traces, limits).is_none(),
            "{index}/{field}"
        );
    }
    let mut extra_budget_field = failed_history();
    extra_budget_field[3]["external_effect"] = json!(true);
    assert!(history::proves_failed_history(&extra_budget_field, &traces, limits).is_none());
    let mut late_graph = failed_history();
    late_graph.insert(7, json!({"t":"graph_resolved", "nodes":["a", "b"]}));
    number_history(&mut late_graph);
    assert!(history::proves_failed_history(&late_graph, &traces, limits).is_none());
    for remove in [0, 3, 5, 7, 9] {
        let mut events = failed_history();
        events.remove(remove);
        assert!(
            history::proves_failed_history(&events, &traces, limits).is_none(),
            "missing {remove}"
        );
    }
    for duplicate in [0, 2, 3, 4, 9] {
        let mut events = failed_history();
        events.insert(duplicate, events[duplicate].clone());
        number_history(&mut events);
        assert!(
            history::proves_failed_history(&events, &traces, limits).is_none(),
            "duplicate {duplicate}"
        );
    }
    let mut restarted = failed_history();
    restarted.extend(failed_history());
    number_history(&mut restarted);
    assert!(history::proves_failed_history(&restarted, &traces, limits).is_none());
}

#[test]
fn failed_history_binds_every_retained_trace_to_its_exact_settlement() {
    let f = Fixture::new();
    let events = failed_history();
    retain_history(&f, &events);
    let traces = f.fold().dependency_traces;
    let limits = JournalLimits::default();
    for change in ["seq", "digest", "owner", "incomplete"] {
        let mut invalid = traces.clone();
        match change {
            "seq" => invalid.owners.get_mut("a").unwrap().seq += 1,
            "digest" => invalid.owners.get_mut("a").unwrap().event_digest = [0; 32],
            "owner" => {
                let trace = invalid.owners.remove("a").unwrap();
                invalid.owners.insert("unpaired".into(), trace);
            }
            _ => invalid.owners.get_mut("a").unwrap().trace = ReadRecorder::new(0).snapshot(),
        }
        assert!(
            history::proves_failed_history(&events, &invalid, limits).is_none(),
            "{change}"
        );
    }
    assert!(
        history::proves_failed_history(
            &events,
            &traces,
            JournalLimits {
                max_event_count: Some(events.len() - 1),
                ..limits
            }
        )
        .is_none()
    );
}
