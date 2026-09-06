mod attempts;
mod completion;
mod core;
mod envelope;
mod guardrails;
mod interaction;
mod llm;
mod resources;
mod run;
mod steps;
mod tools;

pub use attempts::*;
pub use completion::*;
pub use core::*;
pub use envelope::*;
pub use guardrails::*;
pub use interaction::*;
pub use llm::*;
pub use resources::*;
pub use run::*;
pub use steps::*;
pub use tools::*;

#[cfg(test)]
mod tests {
    use super::*;
    use qcg_types::NodePath;
    use serde_json::json;

    #[test]
    fn run_event_decodes_flat_step_finished_into_closed_data() {
        let value = json!({
            "t": "step_finished",
            "seq": 7,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-7",
            "node": "write_config",
            "status": "success",
            "files": [],
            "output": { "ok": true },
            "output_name": "write_config"
        });
        let event = RunEvent::from_flat(&value).expect("event should decode");

        assert_eq!(
            event.path.as_ref().map(NodePath::as_str),
            Some("write_config")
        );
        match event.data {
            RunEventData::StepFinished(data) => {
                assert_eq!(data.status, StepStatus::Success);
                assert_eq!(data.output, Some(json!({ "ok": true })));
            }
            other => panic!("expected typed step_finished, got {other:?}"),
        }
    }

    #[test]
    fn known_event_data_rejects_unknown_fields() {
        let value = json!({
            "t": "step_skipped",
            "seq": 1,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-1",
            "node": "skip",
            "reason": {
                "code": "when_false",
                "message": "condition was false"
            },
            "unexpected": true
        });
        let error = RunEvent::from_flat(&value).expect_err("known event data must be closed");
        assert!(error.contains("unknown field"));
    }

    #[test]
    fn command_resource_events_decode_as_a_typed_source() {
        let value = json!({
            "t": "resource",
            "seq": 2,
            "ts": "2026-09-01T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-2",
            "name": "generated",
            "type": "exec",
            "source": { "kind": "command", "command": ["printf", "hello"] },
            "sha256": "00",
            "bytes": 5,
            "cache": "not_applicable",
            "trust": "untrusted",
            "llm_visible": true
        });
        let event = RunEvent::from_flat(&value).expect("command resource event should decode");
        assert!(matches!(
            event.data,
            RunEventData::Resource(ResourceEventData {
                source: ResourceSource::Command { command },
                ..
            }) if command == ["printf", "hello"]
        ));
    }

    #[test]
    fn specialist_events_use_the_node_envelope_and_closed_typed_data() {
        let delegated = json!({
            "t": "agent_delegated",
            "seq": 1,
            "ts": "2026-08-31T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-1",
            "node": "research",
            "agent": "parallel_researcher",
            "tool_call_id": "call-delegate-1",
            "tools": ["parallel_search"],
            "max_calls": 2,
            "max_iterations": 4,
            "max_tokens_total": 10000,
            "max_tool_calls_total": 3
        });
        let event = RunEvent::from_flat(&delegated).expect("delegated event should decode");
        assert_eq!(event.path.as_ref().map(NodePath::as_str), Some("research"));
        assert!(matches!(event.data, RunEventData::AgentDelegated(_)));

        let llm_call = json!({
            "t": "llm_call",
            "seq": 2,
            "ts": "2026-08-31T00:00:01Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-2",
            "node": "research",
            "provider": "fake",
            "model": "fake",
            "max_tokens": 128,
            "agent": "parallel_researcher",
            "tokens": { "input": 10, "output": 2 },
            "cost_microusd": 0
        });
        let event = RunEvent::from_flat(&llm_call).expect("specialist LLM event should decode");
        match event.data {
            RunEventData::LlmCall(data) => {
                assert_eq!(data.agent.as_deref(), Some("parallel_researcher"));
            }
            other => panic!("expected typed llm_call, got {other:?}"),
        }
    }

    #[test]
    fn failed_tool_events_decode_typed_phase_status_and_error() {
        let value = json!({
            "t": "tool_call",
            "seq": 3,
            "ts": "2026-08-31T00:00:02Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-3",
            "node": "research",
            "tool": "parallel_search",
            "id": "call-1",
            "status": "failed",
            "phase": "execution",
            "agent": "parallel_researcher",
            "error": { "code": "execution_failed", "message": "transport failed" },
            "duration_ms": 7,
            "arguments": { "objective": "research" },
            "result": null,
            "sources": [],
            "truncated": false
        });
        let event = RunEvent::from_flat(&value).expect("typed tool failure should decode");
        match event.data {
            RunEventData::ToolCall(data) => {
                assert_eq!(data.status, ToolCallStatus::Failed);
                assert_eq!(data.phase, ToolCallPhase::Execution);
                assert_eq!(
                    data.error.expect("error details").code,
                    ToolCallErrorCode::ExecutionFailed
                );
            }
            other => panic!("expected typed tool_call, got {other:?}"),
        }
    }

    #[test]
    fn context_compaction_events_decode_prompt_and_transcript_shapes() {
        let prompt = json!({
            "t": "context_compacted",
            "seq": 4,
            "ts": "2026-08-31T00:00:03Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-4",
            "node": "generate",
            "policy": "truncate_tail",
            "original_bytes": 4096,
            "final_bytes": 1024,
            "limit_bytes": 1024
        });
        let event = RunEvent::from_flat(&prompt).expect("prompt compaction should decode");
        assert!(matches!(
            event.data,
            RunEventData::ContextCompacted(ContextCompactedEventData::Prompt(data))
                if data.policy == ContextCompactionPolicy::TruncateTail
                    && data.original_bytes == 4096
        ));

        let transcript = json!({
            "t": "context_compacted",
            "seq": 5,
            "ts": "2026-08-31T00:00:04Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-5",
            "node": "research",
            "scope": "agent_transcript",
            "policy": "truncate_head",
            "original_bytes": 8192,
            "final_bytes": 2048,
            "limit_bytes": 2048,
            "compacted_tool_results": 3,
            "compacted_messages": 1
        });
        let event = RunEvent::from_flat(&transcript).expect("transcript compaction should decode");
        assert!(matches!(
            event.data,
            RunEventData::ContextCompacted(ContextCompactedEventData::RequestOrTranscript(data))
                if data.scope == ContextCompactionScope::AgentTranscript
                    && data.compacted_tool_results == 3
                    && data.compacted_messages == 1
        ));

        let request = json!({
            "t": "context_compacted",
            "seq": 6,
            "ts": "2026-08-31T00:00:05Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-6",
            "node": "research",
            "scope": "request",
            "policy": "truncate_tail",
            "original_bytes": 16384,
            "final_bytes": 4096,
            "limit_bytes": 4096,
            "compacted_tool_results": 2,
            "compacted_messages": 0
        });
        let event = RunEvent::from_flat(&request).expect("request compaction should decode");
        assert!(matches!(
            event.data,
            RunEventData::ContextCompacted(ContextCompactedEventData::RequestOrTranscript(data))
                if data.scope == ContextCompactionScope::Request
                    && data.compacted_tool_results == 2
        ));

        let mut invalid = transcript;
        invalid["unexpected"] = json!(true);
        let error = RunEvent::from_flat(&invalid)
            .expect_err("closed context compaction payload should reject unknown fields");
        assert!(!error.is_empty(), "{error}");
    }

    #[test]
    fn agent_handoff_and_llm_route_failure_events_decode_closed_payloads() {
        let handoff = json!({
            "t": "agent_handoff",
            "seq": 6,
            "ts": "2026-08-31T00:00:05Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-6",
            "node": "research",
            "agent": "parallel_researcher",
            "tool_call_id": "call-handoff-1"
        });
        let event = RunEvent::from_flat(&handoff).expect("agent handoff should decode");
        assert!(matches!(
            event.data,
            RunEventData::AgentHandoff(AgentHandoffEventData { agent, tool_call_id })
                if agent == "parallel_researcher" && tool_call_id == "call-handoff-1"
        ));

        let route_failed = json!({
            "t": "llm_route_failed",
            "seq": 7,
            "ts": "2026-08-31T00:00:06Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-7",
            "node": "research",
            "provider": "openai",
            "model": "gpt-5",
            "attempt": 1,
            "kind": { "http_status": 503 }
        });
        let event = RunEvent::from_flat(&route_failed).expect("route failure should decode");
        assert!(matches!(
            event.data,
            RunEventData::LlmRouteFailed(LlmRouteFailedEventData {
                kind: LlmRouteFailureKind::HttpStatus(503),
                ..
            })
        ));

        let mut invalid = route_failed;
        invalid["unexpected"] = json!(true);
        let error = RunEvent::from_flat(&invalid)
            .expect_err("closed route failure payload should reject unknown fields");
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn unknown_event_kind_preserves_data() {
        let value = json!({
            "t": "third_party.progress",
            "seq": 3,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-3",
            "percent": 50
        });
        let event = RunEvent::from_flat(&value).expect("unknown event should decode");
        match event.data {
            RunEventData::Unknown(data) => assert_eq!(data, json!({ "percent": 50 })),
            other => panic!("expected unknown event data, got {other:?}"),
        }
    }

    #[test]
    fn flat_event_requires_complete_trace_identity() {
        let base = json!({
            "t": "third_party.progress",
            "seq": 1,
            "ts": "2026-07-05T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "trace-1",
            "span_id": "span-1"
        });
        for field in ["run_id", "trace_id", "span_id"] {
            let mut value = base.clone();
            value.as_object_mut().expect("object").remove(field);
            let error = RunEvent::from_flat(&value).expect_err("identity field must be required");
            assert!(error.contains(field), "{error}");
        }
    }
}
