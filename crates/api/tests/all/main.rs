mod run_event_reference;
mod schema_components;

#[test]
fn run_resumed_accepts_legacy_and_versioned_step_budget_baselines() {
    let envelope = serde_json::json!({
        "seq": 1,
        "ts": "2026-11-14T00:00:00Z",
        "run_id": "run",
        "trace_id": "0123456789abcdef0123456789abcdef",
        "span_id": "0000000000000001",
        "t": "run_resumed"
    });
    api::RunEvent::from_flat(&envelope).expect("historical resume payload remains valid");
    let mut current = envelope;
    current["step_budget_baseline"] = serde_json::json!({
        "version": 1,
        "used": 4,
        "charged": 2
    });
    api::RunEvent::from_flat(&current).expect("new boundary payload is typed");
    current["step_budget_baseline"]["version"] = serde_json::json!(2);
    // Version semantics are enforced by the state fold, while the event
    // schema remains extensible enough to carry a future version to it.
    api::RunEvent::from_flat(&current).expect("baseline versions remain fold-validated");
}
