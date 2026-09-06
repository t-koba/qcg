use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_engine::read_output_manifest;
use qcg_service::{
    DirectRun, LocalQcgService, read_run_events, read_run_generator_path, read_run_inputs,
    resolve_run_dir, run_meta_dir,
};
use qcg_types::OutputManifest;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) struct ReplayRequest {
    pub(crate) runs_dir: Utf8PathBuf,
    pub(crate) id: String,
    pub(crate) generator: Option<Utf8PathBuf>,
    pub(crate) output: Option<Utf8PathBuf>,
    pub(crate) reuse_seed: bool,
    pub(crate) answers: BTreeMap<String, Value>,
    pub(crate) confirmations: BTreeMap<String, bool>,
    pub(crate) json_output: bool,
    pub(crate) providers_path: Option<Utf8PathBuf>,
}

pub(crate) async fn replay_run(request: ReplayRequest) -> Result<()> {
    let ReplayRequest {
        runs_dir,
        id,
        generator,
        output,
        reuse_seed,
        answers,
        confirmations,
        json_output,
        providers_path,
    } = request;
    let original_dir = resolve_run_dir(&runs_dir, &id)?;
    let inputs = read_run_inputs(&original_dir)?;
    let replay_seed = if reuse_seed {
        Some(replay_seed_from_journal(&original_dir)?)
    } else {
        None
    };
    let generator_path = match generator {
        Some(path) => path.clone(),
        None => read_run_generator_path(&original_dir)?,
    };
    let output_dir = match output {
        Some(output) => output,
        None => {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_millis();
            runs_dir.join(format!("{id}-replay-{timestamp}"))
        }
    };
    let service = LocalQcgService::new(Utf8PathBuf::new(), runs_dir.to_path_buf(), providers_path)?;
    let replay_manifest = service
        .run_generator_path(DirectRun {
            generator_path: generator_path.clone(),
            inputs,
            output_dir: output_dir.clone(),
            json_events: false,
            interactive: false,
            answers,
            confirmations,
            llm_seed_override: replay_seed,
        })
        .await?;
    let original_manifest = match read_output_manifest(&run_meta_dir(&original_dir)) {
        Ok(manifest) => Some(manifest),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let comparison = compare_output_manifests(original_manifest.as_ref(), &replay_manifest);
    let result = json!({
        "source_run": id,
        "source_dir": original_dir,
        "generator_path": generator_path,
        "replay_dir": output_dir,
        "matched": comparison.get("matched").and_then(Value::as_bool).unwrap_or(false),
        "comparison": comparison,
        "replay_seed": replay_seed,
        "artifacts": replay_manifest.artifacts,
    });
    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "replayed {id} -> {}",
            result["replay_dir"].as_str().unwrap_or("")
        );
        println!(
            "matched: {}",
            result
                .get("matched")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        );
        if let Some(seed) = replay_seed {
            println!("replay_seed: {seed}");
        }
        if let Some(changes) = result
            .get("comparison")
            .and_then(|value| value.get("changes"))
            .and_then(Value::as_array)
        {
            for change in changes {
                println!("{}", serde_json::to_string(change)?);
            }
        }
    }
    Ok(())
}

pub(crate) async fn export_run_trace(
    runs_dir: &Utf8Path,
    id: &str,
    output: Option<&Utf8Path>,
    endpoint: Option<&str>,
) -> Result<()> {
    let run_dir = resolve_run_dir(runs_dir, id)?;
    let events = read_run_events(&run_dir)?;
    if events.is_empty() {
        anyhow::bail!("run `{id}` has no trace events");
    }
    if events
        .iter()
        .any(|event| event.run_id != id || event.trace_id != events[0].trace_id)
    {
        anyhow::bail!("run `{id}` contains inconsistent trace identity");
    }
    let trace_id = events[0].trace_id.clone();
    let start = event_time_unix_nano(&events[0])?;
    let end = event_time_unix_nano(events.last().expect("events is non-empty"))?.max(start + 1);
    let run_span_id = qcg_api::span_id_for_scope(id, "run");
    let mut spans = vec![json!({
        "traceId": trace_id,
        "spanId": run_span_id,
        "name": format!("qcg.run {id}"),
        "kind": 1,
        "startTimeUnixNano": start.to_string(),
        "endTimeUnixNano": end.to_string(),
        "attributes": [otlp_string_attribute("qcg.run.id", id)],
        "status": { "code": 1 }
    })];
    let mut node_ranges = BTreeMap::<String, (u128, u128)>::new();
    for event in &events {
        let Some(node) = event.path.as_ref().map(qcg_types::NodePath::as_str) else {
            continue;
        };
        let timestamp = event_time_unix_nano(event)?;
        node_ranges
            .entry(node.to_string())
            .and_modify(|range| range.1 = timestamp.max(range.1))
            .or_insert((timestamp, timestamp));
    }
    for (node, (node_start, node_end)) in &node_ranges {
        spans.push(json!({
            "traceId": trace_id,
            "spanId": qcg_api::span_id_for_scope(id, &format!("step:{node}")),
            "parentSpanId": run_span_id,
            "name": format!("qcg.step {node}"),
            "kind": 1,
            "startTimeUnixNano": node_start.to_string(),
            "endTimeUnixNano": node_end.saturating_add(1).to_string(),
            "attributes": [otlp_string_attribute("qcg.node.id", node)],
            "status": { "code": 1 }
        }));
    }
    for event in &events {
        let seq = event.seq;
        let kind = event.kind.as_str();
        let timestamp = event_time_unix_nano(event)?;
        let parent = event
            .parent_span_id
            .clone()
            .unwrap_or_else(|| run_span_id.clone());
        let mut attributes = vec![
            otlp_string_attribute("qcg.event.kind", kind),
            json!({ "key": "qcg.event.seq", "value": { "intValue": seq.to_string() } }),
        ];
        if let Some(node) = event.path.as_ref().map(qcg_types::NodePath::as_str) {
            attributes.push(otlp_string_attribute("qcg.node.id", node));
        }
        spans.push(json!({
            "traceId": trace_id,
            "spanId": event.span_id,
            "parentSpanId": parent,
            "name": format!("qcg.event {kind}"),
            "kind": 1,
            "startTimeUnixNano": timestamp.to_string(),
            "endTimeUnixNano": timestamp.saturating_add(1).to_string(),
            "attributes": attributes,
            "status": {
                "code": if matches!(kind, "run_error" | "run_canceled") { 2 } else { 1 }
            }
        }));
    }
    let payload = json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [otlp_string_attribute("service.name", "qcg")]
            },
            "scopeSpans": [{
                "scope": { "name": "qcg.harness", "version": env!("CARGO_PKG_VERSION") },
                "spans": spans
            }]
        }]
    });
    let encoded = serde_json::to_vec_pretty(&payload)?;
    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, &encoded)?;
    }
    if let Some(endpoint) = endpoint {
        reqwest::Client::new()
            .post(endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encoded.clone())
            .send()
            .await
            .with_context(|| format!("failed to export trace to `{endpoint}`"))?
            .error_for_status()
            .with_context(|| format!("trace exporter `{endpoint}` rejected the payload"))?;
    }
    if output.is_none() && endpoint.is_none() {
        println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    } else {
        if let Some(path) = output {
            println!("wrote OTLP trace `{path}`");
        }
        if let Some(endpoint) = endpoint {
            println!("exported OTLP trace to `{endpoint}`");
        }
    }
    Ok(())
}

fn event_time_unix_nano(event: &qcg_api::RunEvent) -> Result<u128> {
    let timestamp = chrono::DateTime::parse_from_rfc3339(&event.ts)
        .with_context(|| format!("invalid trace timestamp `{}`", event.ts))?;
    let seconds = timestamp.timestamp();
    if seconds < 0 {
        anyhow::bail!("trace timestamp predates the Unix epoch");
    }
    Ok(seconds as u128 * 1_000_000_000 + u128::from(timestamp.timestamp_subsec_nanos()))
}

fn otlp_string_attribute(key: &str, value: &str) -> Value {
    json!({ "key": key, "value": { "stringValue": value } })
}

pub(crate) fn replay_seed_from_journal(run_dir: &Utf8Path) -> Result<u64> {
    let events = read_run_events(run_dir)?;
    events
        .iter()
        .find_map(|event| match &event.data {
            qcg_api::RunEventData::LlmCall(data) => data.seed,
            _ => None,
        })
        .with_context(|| format!("run `{run_dir}` does not record an LLM seed"))
}

fn compare_output_manifests(original: Option<&OutputManifest>, replay: &OutputManifest) -> Value {
    let Some(original) = original else {
        return json!({
            "matched": false,
            "changes": [{ "kind": "missing_original_outputs" }],
        });
    };
    let mut changes = Vec::new();
    let original_by_path: BTreeMap<_, _> = original
        .artifacts
        .iter()
        .map(|artifact| (artifact.path.as_str(), artifact))
        .collect();
    let replay_by_path: BTreeMap<_, _> = replay
        .artifacts
        .iter()
        .map(|artifact| (artifact.path.as_str(), artifact))
        .collect();
    for (path, original_artifact) in &original_by_path {
        match replay_by_path.get(path) {
            Some(replay_artifact) => {
                if original_artifact.sha256 != replay_artifact.sha256
                    || original_artifact.bytes != replay_artifact.bytes
                {
                    changes.push(json!({
                        "kind": "changed",
                        "path": path,
                        "original": {
                            "sha256": original_artifact.sha256,
                            "bytes": original_artifact.bytes,
                        },
                        "replay": {
                            "sha256": replay_artifact.sha256,
                            "bytes": replay_artifact.bytes,
                        },
                    }));
                }
            }
            None => changes.push(json!({ "kind": "missing_in_replay", "path": path })),
        }
    }
    for path in replay_by_path.keys() {
        if !original_by_path.contains_key(path) {
            changes.push(json!({ "kind": "new_in_replay", "path": path }));
        }
    }
    json!({
        "matched": changes.is_empty(),
        "changes": changes,
    })
}
