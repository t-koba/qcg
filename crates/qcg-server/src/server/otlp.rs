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
        spans.push(json!({
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": event.get("parent_span_id").and_then(Value::as_str).unwrap_or(""),
            "name": format!("qcg.event {kind}"),
            "kind": 1,
            "startTimeUnixNano": unix_nano.to_string(),
            "endTimeUnixNano": unix_nano.saturating_add(1).to_string(),
            "attributes": attributes,
            "status": {
                "code": if matches!(kind, "run_error" | "run_canceled" | "run_interrupted") { 2 } else { 1 }
            }
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
                Ok(()) => {
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

/// Posts one batch. Non-2xx responses and transport errors are reported so
/// the caller can retry without advancing the cursor.
async fn export_batch(
    client: &reqwest::Client,
    endpoint: &str,
    run_id: &str,
    events: &[Value],
) -> Result<(), String> {
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
    Ok(())
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
}
