//! Resident OTLP/HTTP JSON exporter for run records.
//!
//! Delivery is best-effort by design: an unreachable collector is logged and
//! retried on the next tick with the cursor unchanged, and it never affects
//! run execution or settlement. Event spans are exported incrementally per
//! run so a long-running process streams traces without buffering a whole
//! journal. The endpoint and cadence are deployment policy resolved once at
//! boot (ADR 0001); requests never re-read the environment.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) static EXPORT_FAILURES: AtomicU64 = AtomicU64::new(0);
pub(crate) static REJECTED_SPANS: AtomicU64 = AtomicU64::new(0);
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
        policy::redact_urls_in_text(&self.endpoint)
    }
}

/// Resolves `OTLP_ENDPOINT` / `OTLP_INTERVAL_MS`. Unset endpoint
/// disables export; a lone interval or an invalid value refuses boot instead
/// of silently degrading.
pub(crate) fn resolve_otlp_config() -> Result<Option<OtlpConfig>, String> {
    resolve_otlp_config_values(
        std::env::var("OTLP_ENDPOINT").ok(),
        std::env::var("OTLP_INTERVAL_MS").ok(),
    )
}

fn resolve_otlp_config_values(
    endpoint: Option<String>,
    interval: Option<String>,
) -> Result<Option<OtlpConfig>, String> {
    let endpoint = match endpoint {
        None => {
            if interval.is_some() {
                return Err(
                    "OTLP_INTERVAL_MS is set without OTLP_ENDPOINT; set both or neither"
                        .to_string(),
                );
            }
            return Ok(None);
        }
        Some(value) => {
            let parsed = url::Url::parse(&value)
                .map_err(|error| format!("invalid OTLP_ENDPOINT `{value}`: {error}"))?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(format!(
                    "invalid OTLP_ENDPOINT `{value}`: scheme must be http or https"
                ));
            }
            if parsed.host_str().is_none() {
                return Err(format!(
                    "invalid OTLP_ENDPOINT `{value}`: a host is required"
                ));
            }
            value
        }
    };
    let interval = match interval {
        None => Duration::from_secs(5),
        Some(value) => match value.parse::<u64>() {
            Ok(millis) if millis >= 100 => Duration::from_millis(millis),
            _ => {
                return Err(format!(
                    "invalid OTLP_INTERVAL_MS `{value}`: must be an integer of at least 100"
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
            string_attribute("run.id", run_id),
            string_attribute("event.kind", kind),
            json!({ "key": "event.seq", "value": { "intValue": seq.to_string() } }),
        ];
        if let Some(node) = event.get("node").and_then(Value::as_str) {
            attributes.push(string_attribute("node.id", node));
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
            "name": format!("event {kind}"),
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
                "attributes": [string_attribute("service.name", &policy::default_service_name())]
            },
            "scopeSpans": [{
                "scope": { "name": "harness", "version": env!("CARGO_PKG_VERSION") },
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
        out.push(("status".to_string(), status.to_string()));
    }
    if let Some(reason) = event.get("reason") {
        if let Some(text) = reason.as_str() {
            out.push(("reason".to_string(), text.to_string()));
        } else {
            if let Some(code) = reason.get("code").and_then(Value::as_str) {
                out.push(("reason.code".to_string(), code.to_string()));
            }
            if let Some(message) = reason.get("message").and_then(Value::as_str) {
                out.push(("reason.message".to_string(), message.to_string()));
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
    let mut cursors: BTreeMap<String, service::EventCursor> = BTreeMap::new();
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
        export_cycle(
            &state.service,
            &client,
            &config,
            &mut cursors,
            &state.shutdown,
        )
        .await;
    }
}

// One independently testable observation cycle. Execution never depends on it.
async fn export_cycle(
    service: &service::LocalService,
    client: &reqwest::Client,
    config: &OtlpConfig,
    cursors: &mut BTreeMap<String, service::EventCursor>,
    shutdown: &tokio_util::sync::CancellationToken,
) {
    let runs = match service.observable_run_ids().await {
        Ok(runs) => runs,
        Err(error) => {
            tracing::warn!(%error, "OTLP exporter could not list runs");
            return;
        }
    };
    let present: BTreeSet<_> = runs.iter().cloned().collect();
    cursors.retain(|id, _| present.contains(id));
    use futures_util::StreamExt;
    let jobs = runs
        .into_iter()
        .map(|run| {
            let cursor = cursors.get(&run).cloned().unwrap_or_default();
            (run, cursor)
        })
        .collect::<Vec<_>>();
    let mut exports = futures_util::stream::iter(jobs)
        .map(|(run, cursor)| {
            let service = service.clone();
            let endpoint = &config.endpoint;
            async move {
                let result = async {
                    let batch = service
                        .read_event_batch(&run, cursor)
                        .await
                        .map_err(|error| error.to_string())?;
                    let outcome = if batch.events.is_empty() {
                        None
                    } else {
                        Some(export_batch(client, endpoint, &run, &batch.events).await?)
                    };
                    Ok::<_, String>((batch.cursor, outcome))
                }
                .await;
                (run, result)
            }
        })
        .buffer_unordered(8);
    loop {
        let next = tokio::select! {_=shutdown.cancelled()=>return, next=exports.next()=>next};
        let Some((run, result)) = next else {
            break;
        };
        match result {
            Ok((cursor, outcome)) => {
                if let Some(outcome) = outcome {
                    REJECTED_SPANS.fetch_add(outcome.rejected_spans as u64, Ordering::Relaxed);
                    if outcome.rejected_spans > 0 || !outcome.error_message.is_empty() {
                        tracing::warn!(run_id=%run,rejected_spans=outcome.rejected_spans,error_message=%outcome.error_message,"OTLP partial success advances accepted spans without full resend");
                    }
                }
                cursors.insert(run, cursor);
            }
            Err(error) => {
                EXPORT_FAILURES.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(run_id=%run,%error,"OTLP export failed; batch will retry");
            }
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
    parse_partial_success(&body)
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
/// full success. Malformed nonempty bodies fail as protocol errors; rejection counts
/// must be nonnegative integers.
fn parse_partial_success(body: &[u8]) -> Result<ExportOutcome, String> {
    let value: Value = if body.iter().all(u8::is_ascii_whitespace) {
        json!({})
    } else {
        serde_json::from_slice(body).map_err(|e| format!("invalid OTLP response: {e}"))?
    };
    let fields = value.as_object().ok_or("OTLP response must be an object")?;
    if fields
        .keys()
        .any(|key| !matches!(key.as_str(), "partialSuccess" | "partial_success"))
    {
        return Err("unrecognized OTLP response field".into());
    }
    let mut outcome = ExportOutcome {
        rejected_spans: 0,
        error_message: String::new(),
    };
    if let Some(partial) = value
        .get("partialSuccess")
        .or_else(|| value.get("partial_success"))
    {
        let fields = partial
            .as_object()
            .ok_or("OTLP partialSuccess must be an object")?;
        if fields.keys().any(|key| {
            !matches!(
                key.as_str(),
                "rejectedSpans" | "rejected_spans" | "errorMessage" | "error_message"
            )
        }) {
            return Err("unrecognized OTLP partialSuccess field".into());
        }
        if let Some(count) = partial
            .get("rejectedSpans")
            .or_else(|| partial.get("rejected_spans"))
        {
            outcome.rejected_spans = count
                .as_i64()
                .or_else(|| count.as_str().and_then(|s| s.parse().ok()))
                .filter(|count| *count >= 0)
                .ok_or("invalid OTLP rejectedSpans")?;
        }
        if let Some(message) = partial
            .get("errorMessage")
            .or_else(|| partial.get("error_message"))
        {
            outcome.error_message = message
                .as_str()
                .ok_or("invalid OTLP errorMessage")?
                .to_owned();
        }
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_and_interval_are_resolved_strictly() {
        let resolve = |endpoint: Option<&str>, interval: Option<&str>| {
            resolve_otlp_config_values(endpoint.map(str::to_owned), interval.map(str::to_owned))
        };
        assert_eq!(resolve(None, None).unwrap(), None);
        assert!(
            resolve(None, Some("1000"))
                .unwrap_err()
                .contains("OTLP_INTERVAL_MS")
        );
        assert!(
            resolve(Some("ftp://collector"), None)
                .unwrap_err()
                .contains("scheme")
        );
        assert_eq!(
            resolve(Some("https://collector.example/v1/traces"), None)
                .unwrap()
                .unwrap()
                .interval,
            Duration::from_secs(5)
        );
        assert!(resolve(Some("https://collector.example/v1/traces"), Some("99")).is_err());
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
        assert_eq!(spans[0]["name"], "event run_started");
        assert_eq!(spans[1]["parentSpanId"], "cd".repeat(8));
        assert_eq!(spans[1]["startTimeUnixNano"], "1767225601000000000");
        let attributes = spans[1]["attributes"].as_array().expect("attributes");
        assert!(attributes.iter().any(|attribute| {
            attribute["key"] == "node.id" && attribute["value"]["stringValue"] == "build"
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
                .any(|a| a["key"] == "status" && a["value"]["stringValue"] == "failed"),
            "original status must be preserved"
        );
        assert!(
            attrs
                .iter()
                .any(|a| a["key"] == "reason.code"
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
        )
        .unwrap();
        assert_eq!(outcome.rejected_spans, 3);
        assert_eq!(outcome.error_message, "quota");
        let full = parse_partial_success(b"").unwrap();
        assert_eq!(full.rejected_spans, 0);
        let snake = parse_partial_success(br#"{"partial_success":{"rejected_spans":2}}"#).unwrap();
        assert_eq!(snake.rejected_spans, 2);
        for body in [
            b"not json".as_slice(),
            b"[]",
            br#"{"partialSuccess":true}"#,
            br#"{"partialSuccess":{"rejectedSpans":-1}}"#,
        ] {
            assert!(parse_partial_success(body).is_err());
        }
    }

    /// Minimal collector double: serves one scripted response per test.
    fn serve_collector_once(
        status: u16,
        body: &'static [u8],
    ) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!(
            "http://{}/v1/traces",
            listener.local_addr().expect("address")
        );
        let handle = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "collector received no request before deadline"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("collector accept: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
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
    async fn observation_cycles_isolate_corruption_advance_partial_success_and_prune_gc() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("qcg-otlp-cycle-{}", uuid::Uuid::now_v7()));
            let service =
                crate::tests::single_run_service(root.join("generators"), root.join("runs"));
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            let shutdown = tokio_util::sync::CancellationToken::new();
            let mut cursors = BTreeMap::new();
            let bad = root.join("runs/bad/meta");
            std::fs::create_dir_all(&bad).unwrap();
            std::fs::write(bad.join("journal.jsonl"), "{bad json\n").unwrap();
            for iteration in 0..8 {
                let id = format!("good-{iteration}");
                let meta = root.join("runs").join(&id).join("meta");
                std::fs::create_dir_all(&meta).unwrap();
                let mut event = sample_events().remove(0);
                event["run_id"] = json!(id);
                // Use an internal envelope without a public payload schema;
                // the observation path must not fold execution state.
                event["t"] = json!("budget_charged");
                event["amount"] = json!(0);
                std::fs::write(meta.join("journal.jsonl"), format!("{event}\n")).unwrap();
                let (endpoint, collector) =
                    serve_collector_once(200, br#"{"partialSuccess":{"rejectedSpans":1}}"#);
                let config = OtlpConfig {
                    endpoint,
                    interval: Duration::from_millis(100),
                };
                export_cycle(&service, &client, &config, &mut cursors, &shutdown).await;
                collector.join().unwrap();
                assert_eq!(
                    cursors.len(),
                    1,
                    "bad run must not block or retain a cursor"
                );
                assert!(cursors.contains_key(&id));
                // The collector is now closed. A blind resend would fail,
                // while the advanced cursor reads no events and stays put.
                export_cycle(&service, &client, &config, &mut cursors, &shutdown).await;
                let empty = service
                    .read_event_batch(&id, cursors[&id].clone())
                    .await
                    .unwrap();
                assert!(empty.events.is_empty());
                std::fs::remove_dir_all(meta.parent().unwrap()).unwrap();
                export_cycle(&service, &client, &config, &mut cursors, &shutdown).await;
                assert!(cursors.is_empty(), "GC must prune cursors on every cycle");
            }
            drop(service);
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .expect("OTLP observation cycles exceeded the whole-test deadline");
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
