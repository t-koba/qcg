use camino::Utf8PathBuf;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

fn seed_run(root: &Utf8PathBuf, id: &str, status: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event("run_finished", json!({ "status": status, "metrics": {} }))
        .unwrap();
    drop(journal);
}

fn seed_unsettled_run(root: &Utf8PathBuf, id: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    drop(journal);
}

fn seed_failed_step_run(root: &Utf8PathBuf, id: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_finished",
            json!({
                "node": "build",
                "status": "failed",
                "reason": { "code": "execution_failed", "message": "boom" },
            }),
        )
        .unwrap();
    journal
        .event("run_finished", json!({ "status": "failed", "metrics": {} }))
        .unwrap();
    drop(journal);
}

fn seed_operation_attempts_run(root: &Utf8PathBuf, id: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    for (index, status) in ["success", "clean", "indeterminate"]
        .into_iter()
        .enumerate()
    {
        let mut payload = json!({
            "node": "build",
            "operation_id": format!("op-{index}"),
            "status": status,
        });
        if status != "success" {
            payload["reason"] = json!("effect failed");
        }
        journal.event("operation_finished", payload).unwrap();
    }
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);
}

fn trace(root: &Utf8PathBuf, id: &str) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "trace", id, "--runs-dir"])
        .arg(root.join("runs"))
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

// One bounded loopback collector reply per invocation; the test thread
// services the listener while the CLI child runs, with no external network.
fn trace_with_collector(root: &Utf8PathBuf, id: &str, status: &str, body: &str) -> Output {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
    let response = format!(
        "{status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
    command
        .args(["runs", "trace", id, "--runs-dir"])
        .arg(root.join("runs"))
        .args(["--otlp-endpoint", &endpoint])
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false");
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        command.env_remove(name);
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let (mut socket, _) = loop {
        match listener.accept() {
            Ok(socket) => break socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                    let _ = child.kill();
                    let output = child.wait_with_output().unwrap();
                    panic!(
                        "no OTLP request received: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("{error}"),
        }
    };
    // On Windows the accepted socket can inherit the listener's nonblocking
    // mode, which would turn the first header read into WouldBlock before the
    // collector request arrives. Make it blocking before applying timeouts.
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).unwrap();
        request.push(byte[0]);
        assert!(request.len() < 8192);
    }
    assert!(String::from_utf8_lossy(&request).starts_with("POST /v1/traces "));
    // Drain the request body before replying. A real collector consumes the
    // whole payload first; closing a socket with unread request bytes makes
    // Windows send RST (os error 10054), which loses the response for the
    // client even though the reply was written before the close.
    let content_length = String::from_utf8_lossy(&request)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .expect("trace POST has Content-Length");
    assert!(content_length <= 4 * 1024 * 1024);
    let mut body = vec![0_u8; content_length];
    socket.read_exact(&mut body).unwrap();
    socket.write_all(response.as_bytes()).unwrap();
    drop(socket);
    child.wait_with_output().unwrap()
}

fn spans_named<'a>(payload: &'a Value, name: &str) -> Vec<&'a Value> {
    payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|span| span["name"] == name)
        .collect()
}

fn span<'a>(payload: &'a Value, name: &str) -> &'a Value {
    payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap()
        .iter()
        .find(|span| span["name"] == name)
        .unwrap_or_else(|| panic!("span `{name}` missing from trace"))
}

fn span_code(payload: &Value, name: &str) -> u64 {
    span(payload, name)["status"]["code"].as_u64().unwrap()
}

fn span_string_attribute<'a>(payload: &'a Value, name: &str, key: &str) -> Option<&'a str> {
    span(payload, name)["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|attribute| attribute["key"] == key)
        .map(|attribute| attribute["value"]["stringValue"].as_str().unwrap())
}

#[test]
fn failed_run_exports_error_on_root_and_terminal_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-failed-{}", uuid::Uuid::now_v7()));
    let id = "failed-run";
    seed_run(&root, id, "failed");

    let payload = trace(&root, id);
    assert_eq!(span_code(&payload, &format!("run {id}")), 2, "root span");
    assert_eq!(
        span_code(&payload, "event run_finished"),
        2,
        "terminal span"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn successful_run_exports_ok_on_root_and_terminal_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-success-{}", uuid::Uuid::now_v7()));
    let id = "success-run";
    seed_run(&root, id, "success");

    let payload = trace(&root, id);
    assert_eq!(span_code(&payload, &format!("run {id}")), 1, "root span");
    assert_eq!(
        span_code(&payload, "event run_finished"),
        1,
        "terminal span"
    );
    assert_eq!(
        span_string_attribute(&payload, &format!("run {id}"), "error.type"),
        None
    );
    assert_eq!(
        span_string_attribute(&payload, "event run_finished", "error.type"),
        None
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn unsettled_run_exports_unset_on_root_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-unsettled-{}", uuid::Uuid::now_v7()));
    let id = "unsettled-run";
    seed_unsettled_run(&root, id);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, &format!("run {id}")),
        0,
        "root span must not claim OK before the journal settles"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_step_exports_error_on_event_and_step_spans() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-step-failed-{}", uuid::Uuid::now_v7()));
    let id = "step-failed-run";
    seed_failed_step_run(&root, id);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event step_finished"),
        2,
        "failed step event span"
    );
    assert_eq!(span_code(&payload, "step build"), 2, "step span");
    assert_eq!(
        span_string_attribute(&payload, "event step_finished", "error.type"),
        Some("execution_failed")
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_hook_exports_error_on_its_step_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-hook-failed-{}", uuid::Uuid::now_v7()));
    let id = "hook-failed-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_started",
            json!({ "node": "notify", "type": "hook", "attempt": 1 }),
        )
        .unwrap();
    journal
        .event(
            "hook_failed",
            json!({ "hook": "notify", "policy": "warn", "error": "boom" }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event hook_failed"),
        2,
        "hook event span"
    );
    assert_eq!(span_code(&payload, "step notify"), 2, "hook step span");

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn llm_validation_failure_exports_error_on_event_and_step_spans() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-validation-failed-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "validation-failed-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_started",
            json!({ "node": "build", "type": "llm", "attempt": 1 }),
        )
        .unwrap();
    journal
        .event(
            "llm_validation_failed",
            json!({ "node": "build", "attempt": 1, "message": "bad output" }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event llm_validation_failed"),
        2,
        "validation event span"
    );
    assert_eq!(span_code(&payload, "step build"), 2, "validation step span");
    assert_eq!(
        span_string_attribute(&payload, "event llm_validation_failed", "error.type"),
        Some("llm_validation_failed"),
        "no structured reason code falls back to the event kind"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovered_step_uses_its_last_outcome_for_the_step_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-step-recovered-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "step-recovered-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_finished",
            json!({ "node": "build", "status": "failed" }),
        )
        .unwrap();
    journal
        .event(
            "step_finished",
            json!({ "node": "build", "status": "success" }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "step build"),
        1,
        "last step outcome wins"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn audit_degraded_exports_error_without_affecting_run_status() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-audit-degraded-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "audit-degraded-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "audit_degraded",
            json!({ "reason": "write_failure", "record": "llm_delta" }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event audit_degraded"),
        2,
        "audit degraded event span"
    );
    assert_eq!(
        span_code(&payload, &format!("run {id}")),
        1,
        "root span stays OK"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn llm_cost_unresolved_exports_error_without_affecting_run_status() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-llm-cost-unresolved-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "llm-cost-unresolved-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "llm_cost_unresolved",
            json!({
                "node": "build",
                "provider": "openai",
                "model": "gpt-x",
                "tokens": { "input": 10, "output": 5, "cached_input": 0 },
                "liability": "actual_cost_unrepresentable",
            }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event llm_cost_unresolved"),
        2,
        "unresolved LLM cost event span"
    );
    assert_eq!(
        span_code(&payload, &format!("run {id}")),
        1,
        "root span stays OK"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn elapsed_breach_exports_error_without_terminal_run_span() {
    // The durable marker written when grace delivers a pending interaction:
    // exhaustion facts, no terminal run_finished and no structured reason code.
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-elapsed-breach-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "elapsed-breach-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "elapsed_exceeded",
            json!({
                "node": "build",
                "limit_secs": 1,
                "exhaustion": {"resource": "elapsed_seconds", "used": 1, "allowed": 1},
                "during": "interaction",
            }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event elapsed_exceeded"),
        2,
        "budget breach event span"
    );
    assert_eq!(
        span_string_attribute(&payload, "event elapsed_exceeded", "error.type"),
        Some("elapsed_exceeded"),
        "error.type falls back to the kind without a reason code"
    );
    assert_eq!(
        span_code(&payload, &format!("run {id}")),
        0,
        "root span stays UNSET with no terminal outcome"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn operation_attempts_map_only_their_own_spans() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-operation-attempts-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "operation-attempts-run";
    seed_operation_attempts_run(&root, id);

    let payload = trace(&root, id);
    let attempts = spans_named(&payload, "event operation_finished");
    assert_eq!(attempts.len(), 3, "one span per journaled attempt");
    for (span, (expected_code, expected_error_type)) in attempts.into_iter().zip([
        (1_u64, None),
        (2_u64, Some("operation_finished")),
        (2_u64, Some("operation_finished")),
    ]) {
        assert_eq!(span["status"]["code"].as_u64(), Some(expected_code));
        let error_type = span["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|attribute| attribute["key"] == "error.type")
            .map(|attribute| attribute["value"]["stringValue"].as_str().unwrap());
        assert_eq!(error_type, expected_error_type);
    }
    assert_eq!(
        span_code(&payload, "step build"),
        1,
        "enclosing step span stays OK"
    );
    assert_eq!(
        span_code(&payload, &format!("run {id}")),
        1,
        "root span stays OK"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn otlp_collector_reply_controls_report_and_exit_status() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-otlp-result-{}", uuid::Uuid::now_v7()));
    let id = "otlp-collector-run";
    seed_run(&root, id, "success");
    for (status, body, success, expected, forbidden) in [
        (
            "HTTP/1.1 200 OK",
            r#"{"partialSuccess":{"rejectedSpans":"2","errorMessage":"quota"}}"#,
            false,
            &[
                "2 spans rejected",
                "partially accepted",
                "quota",
                "no automatic resend",
            ][..],
            &["exported OTLP trace"][..],
        ),
        (
            "HTTP/1.1 200 OK",
            "{}",
            true,
            &["exported OTLP trace to"][..],
            &["warning:"][..],
        ),
        (
            "HTTP/1.1 200 OK",
            r#"{"partialSuccess":{"errorMessage":"queue near capacity"}}"#,
            true,
            &["exported OTLP trace to", "queue near capacity"][..],
            &[][..],
        ),
        (
            // No Location header: the final response itself is 3xx.
            "HTTP/1.1 300 Multiple Choices",
            "",
            false,
            &["non-success HTTP status (300)"][..],
            &["exported OTLP trace"][..],
        ),
    ] {
        let output = trace_with_collector(&root, id, status, body);
        let report = format!(
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.status.success(),
            success,
            "status `{status}` body `{body}`: {report}"
        );
        for text in expected {
            assert!(
                report.contains(text),
                "status `{status}` body `{body}` must mention `{text}`: {report}"
            );
        }
        for text in forbidden {
            assert!(
                !report.contains(text),
                "status `{status}` body `{body}` must not mention `{text}`: {report}"
            );
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
