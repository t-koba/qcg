mod read;
mod serialize;
mod types;
mod writer;

pub use read::*;
pub use serialize::*;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use serde_json::{Value, json};

    fn test_limits(
        max_event_bytes: usize,
        max_total_bytes: usize,
        max_event_count: usize,
        max_state_bytes: usize,
    ) -> JournalLimits {
        JournalLimits {
            max_event_bytes: Some(max_event_bytes),
            max_total_bytes: Some(max_total_bytes),
            max_event_count: Some(max_event_count),
            max_state_bytes: Some(max_state_bytes),
        }
    }

    #[test]
    fn append_accepts_an_event_at_the_byte_limit() {
        let limits = test_limits(5, 32, 4, 64);
        let mut output = Vec::new();
        let mut stats = JournalStats::default();

        append_serialized_json_line(&mut output, b"12345".to_vec(), &mut stats, limits)
            .expect("an event exactly at the byte limit should be accepted");
        assert_eq!(output, b"12345\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 6,
                events: 1
            }
        );

        let error =
            append_serialized_json_line(&mut output, b"123456".to_vec(), &mut stats, limits)
                .expect_err("an event over the byte limit should be rejected");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "event",
                actual: 6,
                limit: 5,
            }
        ));
        assert_eq!(output, b"12345\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 6,
                events: 1
            }
        );
    }

    #[test]
    fn append_accepts_total_bytes_at_the_limit() {
        let limits = test_limits(5, 12, 4, 64);
        let mut output = Vec::new();
        let mut stats = JournalStats::default();

        append_serialized_json_line(&mut output, b"12345".to_vec(), &mut stats, limits)
            .expect("the first event should fit within the total byte limit");
        append_serialized_json_line(&mut output, b"12345".to_vec(), &mut stats, limits)
            .expect("the total bytes exactly at the limit should be accepted");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 12,
                events: 2
            }
        );

        let error = append_serialized_json_line(&mut output, b"x".to_vec(), &mut stats, limits)
            .expect_err("an event exceeding the total byte limit should be rejected");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "total journal",
                actual: 14,
                limit: 12,
            }
        ));
        assert_eq!(output, b"12345\n12345\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 12,
                events: 2
            }
        );
    }

    #[test]
    fn append_rejects_the_event_after_the_count_limit() {
        let limits = test_limits(8, 32, 2, 64);
        let mut output = Vec::new();
        let mut stats = JournalStats::default();

        append_serialized_json_line(&mut output, b"a".to_vec(), &mut stats, limits)
            .expect("the first event should fit within the count limit");
        append_serialized_json_line(&mut output, b"b".to_vec(), &mut stats, limits)
            .expect("the second event should fit within the count limit");

        let error = append_serialized_json_line(&mut output, b"c".to_vec(), &mut stats, limits)
            .expect_err("the event after the count limit should be rejected");
        assert!(matches!(
            error,
            JournalError::EventCountExceeded {
                actual: 3,
                limit: 2,
            }
        ));
        assert_eq!(output, b"a\nb\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 4,
                events: 2
            }
        );
    }

    #[test]
    fn journal_event_rejects_state_at_the_byte_limit() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-state-limit-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        let empty_state_bytes = serde_json::to_vec(&crate::RunState::default())
            .expect("the default run state should serialize");
        let limits = test_limits(64 * 1024, 128 * 1024, 8, empty_state_bytes.len());
        let journal =
            JournalWriter::create_with_limits(&path, "state-limit-run", false, None, limits)
                .expect("the empty state should fit exactly at the state limit");

        let error = journal
            .event(
                "run_started",
                json!({
                    "generator": "state-limit@1.0.0",
                    "generator_path": "state-limit",
                    "contract_sha256": "abc",
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": "0.1.0",
                    "schema_version": 1,
                }),
            )
            .expect_err("state growth over the limit should reject the event");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "state",
                limit,
                ..
            } if limit == empty_state_bytes.len()
        ));
        let state = journal.state();
        assert_eq!(state.run_id, None);
        assert_eq!(state.last_seq, 0);
        assert!(state.nodes.is_empty());
        assert!(state.checkpoints.is_empty());
        assert!(state.pending.is_none());
        assert!(state.terminal.is_none());
        assert_eq!(
            std::fs::read(&path).expect("journal should be readable"),
            b""
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_rejects_a_large_newline_free_record_while_reading() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-newline-free-limit-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        let limits = test_limits(8, 64, 4, 64);
        std::fs::write(&path, vec![b'x'; limits.max_event_bytes.unwrap_or(8) + 2])
            .expect("the oversized newline-free record should be written");

        let error = read_journal_values(&path, limits)
            .expect_err("an oversized newline-free record should be rejected");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "event",
                actual: 10,
                limit: 8,
            }
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn fold_journal_ignores_only_a_truncated_final_record() {
        let dir =
            std::env::temp_dir().join(format!("qcg-journal-truncated-tail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        std::fs::write(
            &path,
            concat!(
                "{\"t\":\"run_started\",\"run_id\":\"tail-test\",\"trace_id\":\"trace\",\"span_id\":\"span\",\"ts\":\"2026-01-01T00:00:00Z\",\"generator\":\"test@1\",\"generator_path\":\"test\",\"contract_sha256\":\"abc\",\"inputs\":{},\"resource_hashes\":[],\"qcg\":\"0.1.0\",\"schema_version\":1,\"seq\":1}\n",
                "{\"t\":\"step_started\",\"seq\":"
            ),
        )
        .expect("test journal should be written");
        let state = crate::RunState::fold_journal(&path)
            .expect("a truncated final record should be ignored");
        assert_eq!(state.run_id.as_deref(), Some("tail-test"));
        assert_eq!(state.contract_sha256.as_deref(), Some("abc"));
        assert_eq!(state.last_seq, 1);
    }

    #[test]
    fn run_finished_includes_accumulated_metrics() {
        let dir = std::env::temp_dir().join(format!("qcg-journal-metrics-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "metrics-run", false, None).unwrap();
        journal
            .event(
                "run_started",
                json!({
                    "generator": "metrics@1.0.0",
                    "generator_path": "metrics",
                    "contract_sha256": "abc",
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": "0.1.0",
                    "schema_version": 1,
                }),
            )
            .unwrap();
        journal
            .event(
                "step_started",
                json!({ "node": "a", "type": "test", "attempt": 1 }),
            )
            .unwrap();
        journal
            .event("step_finished", json!({ "node": "a", "status": "success" }))
            .unwrap();
        journal
            .event(
                "step_started",
                json!({ "node": "b", "type": "test", "attempt": 1 }),
            )
            .unwrap();
        journal
            .event(
                "step_finished",
                json!({ "node": "b", "status": "check_failed" }),
            )
            .unwrap();
        journal
            .event(
                "step_skipped",
                json!({
                    "node": "c",
                    "reason": qcg_types::FailureDetail::new(
                        qcg_types::FailureCode::DependencyUnsatisfied,
                        "dependency failed",
                    ),
                }),
            )
            .unwrap();
        journal
            .event(
                "repair_attempt_started",
                json!({ "node": "b", "repair": "r", "recheck": "c", "attempt": 1, "max_attempts": 2 }),
            )
            .unwrap();
        journal
            .event(
                "llm_call",
                json!({
                    "node": "r",
                    "provider": "fake",
                    "model": "fake",
                    "max_tokens": 128,
                    "tokens": { "input": 7, "output": 3 },
                    "cost_microusd": 25,
                }),
            )
            .unwrap();
        journal
            .event("run_finished", json!({ "status": "failed" }))
            .unwrap();

        let source = std::fs::read_to_string(&path).unwrap();
        let last = source.lines().last().unwrap();
        let event: Value = serde_json::from_str(last).unwrap();
        assert_eq!(event["t"], "run_finished");
        assert_eq!(event["metrics"]["steps_total"], 2);
        assert_eq!(event["metrics"]["steps_executed"], 2);
        assert_eq!(event["metrics"]["steps_succeeded"], 1);
        assert_eq!(event["metrics"]["steps_failed"], 1);
        assert_eq!(event["metrics"]["steps_skipped"], 1);
        assert_eq!(event["metrics"]["repair_attempts"], 1);
        assert_eq!(event["metrics"]["llm_calls"], 1);
        assert_eq!(event["metrics"]["tokens_input"], 7);
        assert_eq!(event["metrics"]["tokens_output"], 3);
        assert_eq!(event["metrics"]["tokens_total"], 10);
        assert_eq!(event["metrics"]["cost_microusd"], 25);
        assert!(event["metrics"]["duration_ms"].as_u64().is_some());
    }

    #[test]
    fn terminal_error_and_cancel_events_include_accumulated_metrics() {
        for (kind, payload) in [
            ("run_error", json!({ "error": "boom" })),
            (
                "run_canceled",
                json!({ "reason": qcg_types::FailureDetail::new(
                    qcg_types::FailureCode::Canceled,
                    "user canceled",
                ) }),
            ),
        ] {
            let dir = std::env::temp_dir().join(format!(
                "qcg-journal-terminal-{}-{}",
                kind,
                uuid::Uuid::now_v7()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
            let journal = JournalWriter::create(&path, "terminal-run", false, None).unwrap();
            journal
                .event(
                    "run_started",
                    json!({
                        "generator": "terminal@1.0.0",
                        "generator_path": "terminal",
                        "contract_sha256": "abc",
                        "inputs": {},
                        "resource_hashes": [],
                        "qcg": "0.1.0",
                        "schema_version": 1,
                    }),
                )
                .unwrap();
            journal
                .event(
                    "llm_call",
                    json!({
                        "node": "r",
                        "provider": "fake",
                        "model": "fake",
                        "max_tokens": 128,
                        "tokens": { "input": 7, "output": 3, "cached_input": 2 },
                        "cost_microusd": 25,
                    }),
                )
                .unwrap();
            journal.event(kind, payload).unwrap();

            let source = std::fs::read_to_string(&path).unwrap();
            let last = source.lines().last().unwrap();
            let event: Value = serde_json::from_str(last).unwrap();
            assert_eq!(event["t"], kind);
            assert_eq!(event["metrics"]["llm_calls"], 1);
            assert_eq!(event["metrics"]["tokens_input"], 7);
            assert_eq!(event["metrics"]["tokens_output"], 3);
            assert_eq!(event["metrics"]["tokens_cached_input"], 2);
            assert_eq!(event["metrics"]["tokens_total"], 10);
            assert_eq!(event["metrics"]["cost_microusd"], 25);
        }
    }

    #[test]
    fn poisoned_mutexes_do_not_leave_journal_unusable() {
        let dir = std::env::temp_dir().join(format!("qcg-journal-poison-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "poison-run", false, None).unwrap();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = journal.state.lock().unwrap();
            panic!("poison state mutex for recovery test");
        }));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = journal.file.lock().unwrap();
            panic!("poison file mutex for recovery test");
        }));

        journal
            .event("run_error", json!({ "error": "recovered" }))
            .expect("poisoned journal locks should be recoverable");
    }

    #[test]
    fn budget_state_accumulates_across_journal_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-budget-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        {
            let journal = JournalWriter::create(&path, "budget-run", false, None)
                .expect("first journal round should open");
            journal
                .event(
                    "run_started",
                    json!({
                        "run_id": "budget-run",
                        "generator": "budget@1.0.0",
                        "generator_path": "budget",
                        "contract_sha256": "abc",
                        "inputs": {},
                        "resource_hashes": [],
                        "qcg": "0.1.0",
                        "schema_version": 1,
                    }),
                )
                .expect("run should start");
            journal
                .event(
                    "step_started",
                    json!({ "node": "first", "type": "test", "attempt": 1 }),
                )
                .expect("first step should start");
            journal
                .event(
                    "step_finished",
                    json!({ "node": "first", "status": "success" }),
                )
                .expect("first step should finish");
            journal
                .event(
                    "llm_call",
                    json!({
                        "node": "first",
                        "provider": "fake",
                        "model": "fake",
                        "max_tokens": 128,
                        "tokens": { "input": 7, "output": 3 },
                        "cost_microusd": 25,
                    }),
                )
                .expect("first LLM call should be recorded");
        }

        let journal = JournalWriter::create(&path, "budget-run", false, None)
            .expect("second journal round should open");
        journal
            .event("run_resumed", json!({ "run_id": "budget-run" }))
            .expect("run should resume");
        journal
            .event(
                "step_started",
                json!({ "node": "second", "type": "test", "attempt": 1 }),
            )
            .expect("second step should start");
        journal
            .event(
                "step_finished",
                json!({ "node": "second", "status": "success" }),
            )
            .expect("second step should finish");
        journal
            .event(
                "llm_call",
                json!({
                    "node": "second",
                    "provider": "fake",
                    "model": "fake",
                    "max_tokens": 128,
                    "tokens": { "input": 11, "output": 5 },
                    "cost_microusd": 75,
                }),
            )
            .expect("second LLM call should be recorded");
        journal
            .event("run_finished", json!({ "status": "success" }))
            .expect("run should finish");
        let state = journal.state();
        assert_eq!(state.budget.steps_executed, 2);
        assert_eq!(state.budget.steps_succeeded, 2);
        assert_eq!(state.budget.llm_calls, 2);
        assert_eq!(state.budget.tokens_input, 18);
        assert_eq!(state.budget.tokens_output, 8);
        assert_eq!(state.budget.cost_microusd, 100);
        let source = std::fs::read_to_string(&path).expect("journal should be readable");
        let finished: Value = serde_json::from_str(source.lines().last().expect("finished event"))
            .expect("finished event should be JSON");
        assert_eq!(finished["metrics"]["steps_succeeded"], 2);
        assert_eq!(finished["metrics"]["llm_calls"], 2);
        assert_eq!(finished["metrics"]["tokens_total"], 26);
    }

    #[test]
    fn agent_checkpoint_is_durable_and_cleared_by_step_completion() {
        let dir = std::env::temp_dir().join(format!("qcg-agent-checkpoint-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "checkpoint-run", false, None).unwrap();
        journal
            .event(
                "agent_checkpoint",
                json!({
                    "node": "agent",
                    "turn": 1,
                    "phase": "turn_completed",
                    "checkpoint": {"next_turn": 2}
                }),
            )
            .unwrap();
        assert!(
            journal
                .state()
                .checkpoints
                .contains_key(&qcg_types::NodePath::root("agent"))
        );
        journal
            .event(
                "step_finished",
                json!({"node":"agent","status":"success","files":[]}),
            )
            .unwrap();
        assert!(journal.state().checkpoints.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
