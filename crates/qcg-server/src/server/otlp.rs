//! Resident OTLP/HTTP JSON exporter for run records.
//!
//! Delivery is best-effort by design: an unreachable collector is logged and
//! retried on the next tick with the cursor unchanged, and it never affects
//! run execution or settlement. Event spans are exported incrementally per
//! run so a long-running process streams traces without buffering a whole
//! journal. The endpoint and cadence are deployment policy resolved once at
//! boot (ADR 0001); requests never re-read the environment.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::config::AppState;

/// Boot-frozen OTLP configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpConfig {
    pub endpoint: String,
    pub interval: Duration,
}

impl OtlpConfig {
    pub fn redacted_endpoint(&self) -> String {
        qcg_policy::redact_urls_in_text(&self.endpoint)
    }
}

/// Resolves `QCG_OTLP_ENDPOINT` / `QCG_OTLP_INTERVAL_MS`. Unset endpoint
/// disables export; a lone interval or an invalid value refuses boot instead
/// of silently degrading.
pub(crate) fn resolve_otlp_config() -> Result<Option<OtlpConfig>, String> {
    let endpoint = match std::env::var("QCG_OTLP_ENDPOINT") {
        Err(_) => {
            if std::env::var("QCG_OTLP_INTERVAL_MS").is_ok() {
                return Err(
                    "QCG_OTLP_INTERVAL_MS is set without QCG_OTLP_ENDPOINT; set both or neither"
                        .to_string(),
                );
            }
            return Ok(None);
        }
        Ok(value) => {
            let parsed = url::Url::parse(&value)
                .map_err(|error| format!("invalid QCG_OTLP_ENDPOINT `{value}`: {error}"))?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(format!(
                    "invalid QCG_OTLP_ENDPOINT `{value}`: scheme must be http or https"
                ));
            }
            if parsed.host_str().is_none() {
                return Err(format!(
                    "invalid QCG_OTLP_ENDPOINT `{value}`: a host is required"
                ));
            }
            value
        }
    };
    let interval = match std::env::var("QCG_OTLP_INTERVAL_MS") {
        Err(_) => Duration::from_secs(5),
        Ok(value) => match value.parse::<u64>() {
            Ok(millis) if millis >= 100 => Duration::from_millis(millis),
            _ => {
                return Err(format!(
                    "invalid QCG_OTLP_INTERVAL_MS `{value}`: must be an integer of at least 100"
                ));
            }
        },
    };
    Ok(Some(OtlpConfig { endpoint, interval }))
}

/// Builds one OTLP resourceSpans payload for a batch of flat records. Pure so
/// the mapping is unit-testable without a collector. Records without a
/// parseable identity fail the batch instead of exporting fabricated spans.
/// Durable outcomes map to span status via [`span_status_for_event`]: failed
/// and check_failed outcomes are ERROR, success stays OK, and
/// cancel/interrupt/undecided are ERROR with cause attributes (F11) so a
/// failure is never displayed as OK.
pub(crate) fn trace_payload(run_id: &str, events: &[Value]) -> Result<Value, String> {
    let mut spans = Vec::with_capacity(events.len());
    for event in events {
        let kind = event
            .get("t")
            .and_then(Value::as_str)
            .ok_or_else(|| "record kind is required".to_string())?;
        let trace_id = event
            .get("trace_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("record `{kind}` carries no trace_id"))?;
        let span_id = event
            .get("span_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("record `{kind}` carries no span_id"))?;
        let seq = event
            .get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("record `{kind}` carries no seq"))?;
        let timestamp = event
            .get("ts")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("record `{kind}` carries no timestamp"))?;
        let unix_nano = unix_nano(timestamp)?;
        let mut attributes = vec![
            string_attribute("qcg.run.id", run_id),
            string_attribute("qcg.event.kind", kind),
            json!({ "key": "qcg.event.seq", "value": { "intValue": seq.to_string() } }),
        ];
        if let Some(node) = event.get("node").and_then(Value::as_str) {
            attributes.push(string_attribute("qcg.node.id", node));
        }
        let (status_code, status_message) = span_status_for_event(kind, event);
        for (key, value) in status_detail_attributes(event) {
            attributes.push(string_attribute(&key, &value));
        }
        let mut status = json!({ "code": status_code });
        if let Some(message) = status_message {
            status["message"] = Value::String(message);
        }
        spans.push(json!({
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": event.get("parent_span_id").and_then(Value::as_str).unwrap_or(""),
            "name": format!("qcg.event {kind}"),
            "kind": 1,
            "startTimeUnixNano": unix_nano.to_string(),
            "endTimeUnixNano": unix_nano.saturating_add(1).to_string(),
            "attributes": attributes,
            "status": status
        }));
    }
    Ok(json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [string_attribute("service.name", "qcg")]
            },
            "scopeSpans": [{
                "scope": { "name": "qcg.harness", "version": env!("CARGO_PKG_VERSION") },
                "spans": spans
            }]
        }]
    }))
}

fn string_attribute(key: &str, value: &str) -> Value {
    json!({ "key": key, "value": { "stringValue": value } })
}

/// Maps a durable journal outcome to an OTLP span status (F11). The event
/// kind alone is not enough: `run_finished(status="failed")` and
/// `step_finished(status="failed"/"check_failed")` must not export as OK.
/// Returns the numeric OTLP code (1 = OK, 2 = ERROR) plus an optional
/// short message derived from the stored reason.
fn span_status_for_event(kind: &str, event: &Value) -> (u8, Option<String>) {
    let status = event.get("status").and_then(Value::as_str).unwrap_or("");
    // Legacy terminal markers always signal an error outcome.
    if matches!(kind, "run_error" | "run_canceled" | "run_interrupted") {
        return (2, reason_message(event).or_else(|| Some(kind.to_string())));
    }
    match kind {
        "run_finished" => match status {
            "success" => (1, None),
            "failed" | "check_failed" | "error" => (
                2,
                reason_message(event).or_else(|| Some(status.to_string())),
            ),
            // Cancel / interrupt / unknown terminal states are not success:
            // export them as ERROR with the stored cause so monitoring
            // reflects reality (F11-02).
            "canceled" | "cancelled" | "interrupted" => (
                2,
                reason_message(event).or_else(|| Some(status.to_string())),
            ),
            "" => (2, Some("run_finished without status".to_string())),
            other => (2, Some(format!("run_finished status={other}"))),
        },
        "step_finished" => match status {
            "success" | "repaired" | "routed" | "regenerated" | "answered_on_fail" => (1, None),
            "failed"
            | "check_failed"
            | "repair_exhausted"
            | "regenerate_exhausted"
            | "recheck_failed"
            | "error" => (
                2,
                reason_message(event).or_else(|| Some(status.to_string())),
            ),
            "canceled" | "cancelled" | "interrupted" | "skipped" => (
                2,
                reason_message(event).or_else(|| Some(status.to_string())),
            ),
            "" => (2, Some("step_finished without status".to_string())),
            other => (2, Some(format!("step_finished status={other}"))),
        },
        _ => (1, None),
    }
}

fn reason_message(event: &Value) -> Option<String> {
    if let Some(reason) = event.get("reason") {
        if let Some(text) = reason.as_str() {
            return Some(text.to_string());
        }
        if let Some(code) = reason.get("code").and_then(Value::as_str) {
            if let Some(message) = reason.get("message").and_then(Value::as_str) {
                return Some(format!("{code}: {message}"));
            }
            return Some(code.to_string());
        }
        if let Some(message) = reason.get("message").and_then(Value::as_str) {
            return Some(message.to_string());
        }
    }
    event
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Preserves the original durable outcome on the span (F11) so an ERROR
/// mapping stays debuggable and an OK mapping stays verifiable.
fn status_detail_attributes(event: &Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(status) = event.get("status").and_then(Value::as_str) {
        out.push(("qcg.status".to_string(), status.to_string()));
    }
    if let Some(reason) = event.get("reason") {
        if let Some(text) = reason.as_str() {
            out.push(("qcg.reason".to_string(), text.to_string()));
        } else {
            if let Some(code) = reason.get("code").and_then(Value::as_str) {
                out.push(("qcg.reason.code".to_string(), code.to_string()));
            }
            if let Some(message) = reason.get("message").and_then(Value::as_str) {
                out.push(("qcg.reason.message".to_string(), message.to_string()));
            }
        }
    }
    out
}

fn unix_nano(timestamp: &str) -> Result<u128, String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(timestamp)
        .map_err(|error| format!("invalid record timestamp `{timestamp}`: {error}"))?;
    let seconds = parsed.timestamp();
    if seconds < 0 {
        return Err(format!(
            "record timestamp `{timestamp}` predates the Unix epoch"
        ));
    }
    Ok(seconds as u128 * 1_000_000_000 + u128::from(parsed.timestamp_subsec_nanos()))
}

/// Resident export loop. Each tick exports only records after the per-run
/// cursor; a rejected POST leaves the cursor unchanged so the batch retries.
pub(crate) async fn run_exporter(state: Arc<AppState>, config: OtlpConfig) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "OTLP exporter client could not be built; export disabled");
            return;
        }
    };
    let mut cursors: BTreeMap<String, u64> = BTreeMap::new();
    let mut completed: BTreeSet<String> = BTreeSet::new();
    let mut interval = tokio::time::interval(config.interval);
    tracing::info!(
        endpoint = %config.redacted_endpoint(),
        interval_ms = config.interval.as_millis(),
        "resident OTLP exporter started"
    );
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => return,
            _ = interval.tick() => {}
        }
        let runs = match state.service.list_run_items().await {
            Ok(runs) => runs,
            Err(error) => {
                tracing::warn!(%error, "OTLP exporter could not list runs");
                continue;
            }
        };
        for run in runs {
            // Cooperative shutdown inside the scan (F01): a slow collector
            // must not pin service state past the shutdown deadline. Check
            // between runs so aborting the task is a fallback, not the plan.
            if state.shutdown.is_cancelled() {
                return;
            }
            if completed.contains(&run.run_id) {
                continue;
            }
            let terminal = matches!(
                run.state,
                qcg_api::RunStatus::Succeeded
                    | qcg_api::RunStatus::Failed
                    | qcg_api::RunStatus::Canceled
                    | qcg_api::RunStatus::Interrupted
            );
            let run_dir = match state.service.run_dir_for(&run.run_id).await {
                Ok(run_dir) => run_dir,
                Err(error) => {
                    tracing::debug!(run_id = %run.run_id, %error, "OTLP exporter skipped an unreadable run");
                    continue;
                }
            };
            let events = match qcg_service::read_events_with_audit(&run_dir) {
                Ok(events) => events,
                Err(error) => {
                    tracing::warn!(run_id = %run.run_id, %error, "OTLP exporter could not read run records");
                    continue;
                }
            };
            let cursor = cursors.get(&run.run_id).copied().unwrap_or(0);
            let batch: Vec<Value> = events
                .into_iter()
                .filter(|event| event.get("seq").and_then(Value::as_u64).unwrap_or(0) > cursor)
                .collect();
            if batch.is_empty() {
                if terminal {
                    completed.insert(run.run_id.clone());
                }
                continue;
            }
            let max_seq = batch
                .iter()
                .filter_map(|event| event.get("seq").and_then(Value::as_u64))
                .max()
                .unwrap_or(cursor);
            match export_batch(&client, &config.endpoint, &run.run_id, &batch).await {
                Ok(outcome) => {
                    if outcome.rejected_spans > 0 || outcome.rejected_log_records > 0 {
                        // Partial success is not full success (F11-03): the
                        // accepted spans stay accepted (no blind full resend),
                        // but the rejection is observed instead of silently
                        // advancing as success.
                        tracing::warn!(
                            run_id = %run.run_id,
                            endpoint = %config.redacted_endpoint(),
                            rejected_spans = outcome.rejected_spans,
                            rejected_logs = outcome.rejected_log_records,
                            error_message = %outcome.error_message,
                            "OTLP collector partially rejected the batch; accepted spans advance, rejections are recorded"
                        );
                    }
                    cursors.insert(run.run_id.clone(), max_seq);
                    if terminal {
                        completed.insert(run.run_id.clone());
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        run_id = %run.run_id,
                        endpoint = %config.redacted_endpoint(),
                        %error,
                        "OTLP export failed; the batch will retry"
                    );
                }
            }
        }
        // Bound the completion marker set: dropping it only re-reads runs
        // whose cursor already covers every record, so no duplicate spans.
        if completed.len() > 4_096 {
            completed.clear();
        }
    }
}

/// Outcome of one collector POST (F11-03). A 2xx with `partial_success`
/// rejections still advances the cursor for accepted spans (no blind full
/// resend per the OTLP spec) but reports the rejection so monitoring does
/// not mistake it for full success.
#[derive(Debug)]
struct ExportOutcome {
    rejected_spans: i64,
    rejected_log_records: i64,
    error_message: String,
}

/// Posts one batch. Non-2xx responses and transport errors are reported so
/// the caller can retry without advancing the cursor. A 2xx body is read
/// bounded and inspected for OTLP `partial_success`: rejections are
/// reported via [`ExportOutcome`] instead of being treated as full success.
async fn export_batch(
    client: &reqwest::Client,
    endpoint: &str,
    run_id: &str,
    events: &[Value],
) -> Result<ExportOutcome, String> {
    let payload = trace_payload(run_id, events)?;
    let encoded = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    let response = client
        .post(endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(encoded)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "collector rejected the payload: {}",
            response.status()
        ));
    }
    let body = read_bounded_body(response, 64 * 1024).await?;
    Ok(parse_partial_success(&body))
}

/// Reads a collector response body with an explicit bound (F11) so a
/// misbehaving collector cannot grow memory via an unbounded read.
async fn read_bounded_body(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(format!("collector response exceeded {limit} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Parses OTLP/HTTP `partial_success` from a 2xx body. Empty bodies mean
/// full success. Unparseable bodies are treated as full success with no
/// rejections (the transport already succeeded); only well-formed
/// rejection counts are reported.
fn parse_partial_success(body: &[u8]) -> ExportOutcome {
    let trimmed: Vec<u8> = body
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if trimmed.is_empty() {
        return ExportOutcome {
            rejected_spans: 0,
            rejected_log_records: 0,
            error_message: String::new(),
        };
    }
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => {
            return ExportOutcome {
                rejected_spans: 0,
                rejected_log_records: 0,
                error_message: String::new(),
            };
        }
    };
    let mut rejected_spans = 0_i64;
    let mut rejected_logs = 0_i64;
    let mut message = String::new();
    // Traces response shape: { partialSuccess: { rejectedSpans, errorMessage } }
    if let Some(partial) = value
        .get("partialSuccess")
        .or_else(|| value.get("partial_success"))
    {
        rejected_spans = partial
            .get("rejectedSpans")
            .or_else(|| partial.get("rejected_spans"))
            .and_then(Value::as_i64)
            .or_else(|| {
                partial
                    .get("rejectedSpans")
                    .or_else(|| partial.get("rejected_spans"))
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<i64>().ok())
            })
            .unwrap_or(0)
            .max(0);
        if let Some(text) = partial
            .get("errorMessage")
            .or_else(|| partial.get("error_message"))
            .and_then(Value::as_str)
        {
            message = text.to_string();
        }
    }
    // Logs response shape nests under similar keys; check top-level too.
    if let Some(logs_partial) = value
        .get("logsPartialSuccess")
        .or_else(|| value.get("logs_partial_success"))
    {
        rejected_logs = logs_partial
            .get("rejectedLogRecords")
            .or_else(|| logs_partial.get("rejected_log_records"))
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .max(0);
    }
    ExportOutcome {
        rejected_spans,
        rejected_log_records: rejected_logs,
        error_message: message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_and_interval_are_resolved_strictly() {
        // SAFETY: the test binary serializes nothing else that reads these
        // variables; each case sets or clears them before resolving.
        unsafe {
            std::env::remove_var("QCG_OTLP_ENDPOINT");
            std::env::remove_var("QCG_OTLP_INTERVAL_MS");
        }
        assert_eq!(
            resolve_otlp_config().expect("unset endpoint must resolve"),
            None
        );
        unsafe {
            std::env::set_var("QCG_OTLP_INTERVAL_MS", "1000");
        }
        let error = resolve_otlp_config().expect_err("a lone interval must refuse boot");
        assert!(error.contains("QCG_OTLP_INTERVAL_MS"), "{error}");
        unsafe {
            std::env::remove_var("QCG_OTLP_INTERVAL_MS");
            std::env::set_var("QCG_OTLP_ENDPOINT", "ftp://collector");
        }
        let error = resolve_otlp_config().expect_err("non-http schemes must refuse boot");
        assert!(error.contains("scheme"), "{error}");
        unsafe {
            std::env::set_var("QCG_OTLP_ENDPOINT", "https://collector.example/v1/traces");
        }
        assert_eq!(
            resolve_otlp_config().expect("valid endpoint must resolve"),
            Some(OtlpConfig {
                endpoint: "https://collector.example/v1/traces".into(),
                interval: Duration::from_secs(5),
            })
        );
        unsafe {
            std::env::set_var("QCG_OTLP_INTERVAL_MS", "99");
        }
        assert!(
            resolve_otlp_config().is_err(),
            "sub-100ms cadence must refuse"
        );
        unsafe {
            std::env::remove_var("QCG_OTLP_ENDPOINT");
            std::env::remove_var("QCG_OTLP_INTERVAL_MS");
        }
    }

    #[test]
    fn trace_payload_maps_records_to_event_spans() {
        let events = vec![
            json!({
                "t": "run_started",
                "seq": 1,
                "ts": "2026-01-01T00:00:00Z",
                "run_id": "run-1",
                "trace_id": "ab".repeat(16),
                "span_id": "cd".repeat(8),
            }),
            json!({
                "t": "step_finished",
                "seq": 2,
                "ts": "2026-01-01T00:00:01Z",
                "run_id": "run-1",
                "trace_id": "ab".repeat(16),
                "span_id": "ef".repeat(8),
                "parent_span_id": "cd".repeat(8),
                "node": "build",
            }),
        ];
        let payload = trace_payload("run-1", &events).expect("records should map");
        let spans = &payload["resourceSpans"][0]["scopeSpans"][0]["spans"];
        assert_eq!(spans.as_array().map(Vec::len), Some(2));
        assert_eq!(spans[0]["name"], "qcg.event run_started");
        assert_eq!(spans[1]["parentSpanId"], "cd".repeat(8));
        assert_eq!(spans[1]["startTimeUnixNano"], "1767225601000000000");
        let attributes = spans[1]["attributes"].as_array().expect("attributes");
        assert!(attributes.iter().any(|attribute| {
            attribute["key"] == "qcg.node.id" && attribute["value"]["stringValue"] == "build"
        }));
    }

    #[test]
    fn trace_payload_rejects_records_without_identity() {
        let error = trace_payload("run-1", &[json!({ "t": "run_started" })])
            .expect_err("identity is required");
        assert!(error.contains("trace_id"), "{error}");
    }

    #[test]
    fn failed_outcomes_export_as_error_not_ok() {
        // F11-01: run_finished failed and step_finished failed/check_failed
        // must not display as OK; success stays OK.
        let events = vec![
            json!({
                "t": "run_finished", "seq": 1, "ts": "2026-01-01T00:00:00Z",
                "run_id": "run-1", "trace_id": "ab".repeat(16),
                "span_id": "cd".repeat(8), "status": "failed",
                "reason": {"code": "execution_failed", "message": "boom"},
            }),
            json!({
                "t": "step_finished", "seq": 2, "ts": "2026-01-01T00:00:01Z",
                "run_id": "run-1", "trace_id": "ab".repeat(16),
                "span_id": "ef".repeat(8), "node": "build", "status": "failed",
                "reason": {"code": "execution_failed", "message": "boom"},
            }),
            json!({
                "t": "step_finished", "seq": 3, "ts": "2026-01-01T00:00:02Z",
                "run_id": "run-1", "trace_id": "ab".repeat(16),
                "span_id": "ab".repeat(8), "node": "check", "status": "check_failed",
            }),
            json!({
                "t": "run_finished", "seq": 4, "ts": "2026-01-01T00:00:03Z",
                "run_id": "run-1", "trace_id": "ab".repeat(16),
                "span_id": "aa".repeat(8), "status": "success",
            }),
        ];
        let payload = trace_payload("run-1", &events).expect("records should map");
        let spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .expect("spans");
        assert_eq!(spans[0]["status"]["code"], 2, "failed run must be ERROR");
        assert_eq!(spans[1]["status"]["code"], 2, "failed step must be ERROR");
        assert_eq!(spans[2]["status"]["code"], 2, "check_failed must be ERROR");
        assert_eq!(spans[3]["status"]["code"], 1, "success stays OK");
        let attrs = spans[0]["attributes"].as_array().expect("attributes");
        assert!(
            attrs
                .iter()
                .any(|a| a["key"] == "qcg.status" && a["value"]["stringValue"] == "failed"),
            "original status must be preserved"
        );
        assert!(
            attrs.iter().any(|a| a["key"] == "qcg.reason.code"
                && a["value"]["stringValue"] == "execution_failed"),
            "original reason must be preserved"
        );
    }

    #[test]
    fn cancel_and_interrupt_export_as_error_with_cause() {
        // F11-02: cancel/interrupted/undetermined events follow an explicit
        // convention instead of OK.
        for status in ["canceled", "interrupted"] {
            let events = vec![json!({
                "t": "run_finished", "seq": 1, "ts": "2026-01-01T00:00:00Z",
                "run_id": "run-1", "trace_id": "ab".repeat(16),
                "span_id": "cd".repeat(8), "status": status,
            })];
            let payload = trace_payload("run-1", &events).expect("records should map");
            let span = &payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
            assert_eq!(span["status"]["code"], 2, "{status} must not be OK");
        }
    }

    #[test]
    fn partial_success_is_observed_not_full_success() {
        // F11-03: a 200 with partialSuccess rejections is reported, not
        // treated as full success.
        let outcome = parse_partial_success(
            br#"{"partialSuccess":{"rejectedSpans":"3","errorMessage":"quota"}}"#,
        );
        assert_eq!(outcome.rejected_spans, 3);
        assert_eq!(outcome.error_message, "quota");
        let full = parse_partial_success(b"");
        assert_eq!(full.rejected_spans, 0);
        let snake = parse_partial_success(br#"{"partial_success":{"rejected_spans":2}}"#);
        assert_eq!(snake.rejected_spans, 2);
    }

    /// Minimal collector double: serves one scripted response per test.
    fn serve_collector_once(
        status: u16,
        body: &'static [u8],
    ) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
        let endpoint = format!(
            "http://{}/v1/traces",
            listener.local_addr().expect("address")
        );
        let handle = std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && head.len() < 65536 {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            let length = String::from_utf8_lossy(&head)
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    (name.trim().eq_ignore_ascii_case("content-length"))
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            let mut drained = vec![0u8; length];
            let _ = stream.read_exact(&mut drained);
            let reason = if status == 200 { "OK" } else { "Error" };
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            );
            let _ = stream.write_all(body);
        });
        (endpoint, handle)
    }

    fn sample_events() -> Vec<Value> {
        vec![json!({
            "t": "step_finished",
            "seq": 1,
            "ts": "2026-01-01T00:00:00Z",
            "run_id": "run-1",
            "trace_id": "ab".repeat(16),
            "span_id": "cd".repeat(8),
            "node": "build",
            "status": "success",
        })]
    }

    #[tokio::test]
    async fn collector_outcomes_follow_the_defined_policy() {
        // F11-03: full success advances silently, partial success reports
        // rejections without blindly resending, HTTP rejection retries
        // without advancing, and a dead collector errors instead of
        // advancing.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("client should build");
        // Full success: empty body, no rejections.
        let (endpoint, handle) = serve_collector_once(200, b"{}");
        let outcome = export_batch(&client, &endpoint, "run-1", &sample_events())
            .await
            .expect("full success must not error");
        assert_eq!(outcome.rejected_spans, 0);
        handle.join().expect("collector should finish");
        // Partial success: rejections observed, still Ok (accepted spans
        // advance; no blind full resend).
        let (endpoint, handle) = serve_collector_once(
            200,
            br#"{"partialSuccess":{"rejectedSpans":2,"errorMessage":"quota"}}"#,
        );
        let outcome = export_batch(&client, &endpoint, "run-1", &sample_events())
            .await
            .expect("partial success must not error");
        assert_eq!(outcome.rejected_spans, 2);
        assert_eq!(outcome.error_message, "quota");
        handle.join().expect("collector should finish");
        // Rejection: non-2xx errors so the caller retries with the cursor
        // unchanged.
        let (endpoint, handle) = serve_collector_once(500, b"boom");
        let error = export_batch(&client, &endpoint, "run-1", &sample_events())
            .await
            .expect_err("rejection must error");
        assert!(error.contains("500"), "{error}");
        handle.join().expect("collector should finish");
        // Disconnect: a dead collector errors instead of advancing.
        let dead_listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
        let dead_port = dead_listener.local_addr().expect("address").port();
        drop(dead_listener);
        let error = export_batch(
            &client,
            &format!("http://127.0.0.1:{dead_port}/v1/traces"),
            "run-1",
            &sample_events(),
        )
        .await
        .expect_err("disconnect must error");
        assert!(!error.is_empty());
    }
}
