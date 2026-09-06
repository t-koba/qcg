use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::Contract;
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;
use qcg_service::{
    list_run_summaries, read_run_events, read_run_generator_path, resolve_run_dir, run_summary,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(crate) fn list_runs(
    runs_dir: &Utf8Path,
    state: Option<&str>,
    generator: Option<&str>,
) -> Result<()> {
    for summary in list_run_summaries(runs_dir)? {
        if let Some(want) = state
            && summary.status != want
        {
            continue;
        }
        if let Some(want) = generator
            && summary.generator != want
        {
            continue;
        }
        println!(
            "{}\t{}\t{}\t{}",
            summary.run_id, summary.status, summary.generator, summary.started_at
        );
    }
    Ok(())
}

pub(crate) fn show_run(
    runs_dir: &Utf8Path,
    id: &str,
    json_output: bool,
    diagnose: bool,
) -> Result<()> {
    let run_dir = resolve_run_dir(runs_dir, id)?;
    let summary = run_summary(&run_dir)?;
    if json_output {
        if diagnose {
            let diagnosis = diagnose_run(&run_dir)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "summary": summary.to_json(),
                    "diagnosis": diagnosis,
                }))?
            );
        } else {
            println!("{}", serde_json::to_string_pretty(&summary.to_json())?);
        }
        return Ok(());
    }
    println!("run: {id}");
    println!("status: {}", summary.status);
    println!("generator: {}", summary.generator);
    if !summary.started_at.is_empty() {
        println!("started_at: {}", summary.started_at);
    }
    if let Some(finished_at) = &summary.finished_at {
        println!("finished_at: {finished_at}");
    }
    if let Some(inputs) = summary.to_json()["inputs"].as_object()
        && !inputs.is_empty()
    {
        println!("inputs:");
        for (field, value) in inputs {
            if let (Some(name), Some(bytes), Some(sha256)) = (
                value.get("name").and_then(Value::as_str),
                value.get("bytes").and_then(Value::as_u64),
                value.get("sha256").and_then(Value::as_str),
            ) {
                println!("  {field}\t{name}\t{bytes} bytes\t{sha256}");
            } else {
                println!("  {field}\t{value}");
            }
        }
    }
    if !summary.artifacts.is_empty() {
        println!("artifacts:");
        for artifact in &summary.artifacts {
            println!(
                "  {}\t{} bytes\t{}",
                artifact.path, artifact.bytes, artifact.sha256
            );
        }
    }
    match qcg_service::read_run_metrics(&run_dir) {
        Ok(Some(metrics)) => print_run_metrics(&metrics),
        Ok(None) => println!("metrics: no recorded activity yet"),
        Err(error) => println!("metrics: unavailable ({error})"),
    }
    if diagnose {
        let diagnosis = diagnose_run(&run_dir)?;
        print_diagnosis(&diagnosis);
    }
    Ok(())
}

/// Bundle journal failure signals into one ordered diagnosis object.
fn diagnose_run(run_dir: &Utf8Path) -> Result<Value> {
    use qcg_service::read_run_events;
    let events = read_run_events(run_dir)?;
    let mut causes: Vec<Value> = Vec::new();
    for event in &events {
        let node = event
            .path
            .as_ref()
            .map(|path| path.as_str().to_string())
            .unwrap_or_default();
        let data = serde_json::to_value(&event.data).unwrap_or(Value::Null);
        let cause = match event.kind.as_str() {
            "step_finished" => {
                let status = data.get("status").and_then(Value::as_str).unwrap_or("");
                if status == "failed" {
                    Some(json!({
                        "seq": event.seq,
                        "kind": "step_failed",
                        "node": node,
                        "code": data.get("reason").and_then(|reason| reason.get("code")).cloned().unwrap_or(Value::Null),
                        "message": data.get("reason").and_then(|reason| reason.get("message")).and_then(Value::as_str).unwrap_or(""),
                    }))
                } else {
                    None
                }
            }
            "step_skipped" => Some(json!({
                "seq": event.seq,
                "kind": "step_skipped",
                "node": node,
                "code": data.get("reason").and_then(|reason| reason.get("code")).cloned().unwrap_or(Value::Null),
                "message": data.get("reason").and_then(|reason| reason.get("message")).and_then(Value::as_str).unwrap_or(""),
            })),
            "tool_call" => {
                let status = data.get("status").and_then(Value::as_str).unwrap_or("");
                if status == "failed" {
                    Some(json!({
                        "seq": event.seq,
                        "kind": "tool_failed",
                        "node": node,
                        "tool": data.get("tool").cloned().unwrap_or(Value::Null),
                        "phase": data.get("phase").cloned().unwrap_or(Value::Null),
                        "code": data.get("error").and_then(|error| error.get("code")).cloned().unwrap_or(Value::Null),
                        "message": data.get("error").and_then(|error| error.get("message")).and_then(Value::as_str).unwrap_or(""),
                    }))
                } else {
                    None
                }
            }
            "llm_route_failed"
            | "llm_validation_failed"
            | "step_retry"
            | "context_compacted"
            | "guardrail_tripwire"
            | "guardrail_error"
            | "run_error"
            | "run_canceled"
            | "run_interrupted" => Some(json!({
                "seq": event.seq,
                "kind": event.kind,
                "node": node,
                "data": data,
            })),
            _ => None,
        };
        if let Some(cause) = cause {
            causes.push(cause);
        }
    }
    Ok(json!({
        "causes": causes,
        "cause_count": causes.len(),
    }))
}

fn print_diagnosis(diagnosis: &Value) {
    let causes = diagnosis
        .get("causes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if causes.is_empty() {
        println!("diagnosis: no failure signals in journal");
        return;
    }
    println!("diagnosis ({} signals):", causes.len());
    for cause in causes {
        println!(
            "  #{} {} node={} {}",
            cause.get("seq").and_then(Value::as_u64).unwrap_or(0),
            cause
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            cause.get("node").and_then(Value::as_str).unwrap_or("-"),
            cause
                .get("message")
                .and_then(Value::as_str)
                .map_or_else(String::new, |message| message.chars().take(160).collect()),
        );
    }
}

/// Human-readable USD estimate for display; the stored unit stays microUSD.
pub(crate) fn format_usd(cost_microusd: u64) -> String {
    format!("${:.6}", cost_microusd as f64 / 1_000_000.0)
}

fn print_run_metrics(metrics: &qcg_types::RunMetrics) {
    println!(
        "tokens: in={} out={} cached={} total={} ({} calls)",
        metrics.tokens_input,
        metrics.tokens_output,
        metrics.tokens_cached_input,
        metrics.tokens_input.saturating_add(metrics.tokens_output),
        metrics.llm_calls
    );
    println!(
        "cost: {} microUSD ({})",
        metrics.cost_microusd,
        format_usd(metrics.cost_microusd)
    );
    println!("duration_ms: {}", metrics.duration_ms);
}

pub(crate) fn show_costs(
    runs_dir: &Utf8Path,
    state_filter: &Option<String>,
    generator_filter: &Option<String>,
    by_model: bool,
    json_output: bool,
) -> Result<()> {
    #[derive(Debug, Default)]
    struct ModelAggregate {
        provider: String,
        model: String,
        calls: u64,
        tokens_input: u64,
        tokens_output: u64,
        tokens_cached_input: u64,
        cost_microusd: u64,
        input_rate: Option<f64>,
        output_rate: Option<f64>,
        unpriced_calls: u64,
    }

    struct RunCost {
        run_id: String,
        state: String,
        generator: String,
        metrics: qcg_types::RunMetrics,
        unpriced: bool,
    }

    let mut runs: Vec<RunCost> = Vec::new();
    let mut models: BTreeMap<(String, String), ModelAggregate> = BTreeMap::new();
    let mut scanned = 0_usize;
    let entries = std::fs::read_dir(runs_dir)
        .with_context(|| format!("failed to list runs under `{runs_dir}`"))?;
    for entry in entries {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            anyhow::bail!("runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries");
        }
        let entry = entry?;
        let run_dir = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|path| anyhow::anyhow!("run path is not UTF-8: {}", path.display()))?;
        if !run_dir.is_dir() {
            continue;
        }
        let summary = match run_summary(&run_dir) {
            Ok(summary) => summary,
            Err(_) => continue,
        };
        if let Some(want) = state_filter
            && summary.status != *want
        {
            continue;
        }
        if let Some(want) = generator_filter
            && summary.generator != *want
        {
            continue;
        }
        let metrics = match qcg_service::read_run_metrics(&run_dir)? {
            Some(metrics) => metrics,
            None => continue,
        };
        let mut unpriced = false;
        if by_model {
            let contract = read_run_generator_path(&run_dir)
                .ok()
                .and_then(|path| Contract::load(path).ok());
            for event in read_run_events(&run_dir)? {
                let qcg_api::RunEventData::LlmCall(data) = &event.data else {
                    continue;
                };
                let entry = models
                    .entry((data.provider.clone(), data.model.clone()))
                    .or_insert_with(|| ModelAggregate {
                        provider: data.provider.clone(),
                        model: data.model.clone(),
                        ..Default::default()
                    });
                entry.calls = entry.calls.saturating_add(1);
                entry.tokens_input = entry.tokens_input.saturating_add(data.tokens.input);
                entry.tokens_output = entry.tokens_output.saturating_add(data.tokens.output);
                entry.tokens_cached_input = entry
                    .tokens_cached_input
                    .saturating_add(data.tokens.cached_input);
                entry.cost_microusd = entry.cost_microusd.saturating_add(data.cost_microusd);
                let billed = data.tokens.input.saturating_add(data.tokens.output) > 0;
                match contract
                    .as_ref()
                    .and_then(|contract| find_pricing(contract, &data.provider, &data.model))
                {
                    Some((input_rate, output_rate)) => {
                        entry.input_rate = Some(input_rate);
                        entry.output_rate = Some(output_rate);
                    }
                    None if billed && data.cost_microusd == 0 => {
                        entry.unpriced_calls = entry.unpriced_calls.saturating_add(1);
                        unpriced = true;
                    }
                    None => {}
                }
            }
        }
        runs.push(RunCost {
            run_id: summary.run_id,
            state: summary.status,
            generator: summary.generator,
            metrics,
            unpriced,
        });
    }
    runs.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    if json_output {
        let items = runs
            .iter()
            .map(|run| {
                json!({
                    "run_id": run.run_id,
                    "state": run.state,
                    "generator": run.generator,
                    "tokens_input": run.metrics.tokens_input,
                    "tokens_output": run.metrics.tokens_output,
                    "tokens_cached_input": run.metrics.tokens_cached_input,
                    "llm_calls": run.metrics.llm_calls,
                    "cost_microusd": run.metrics.cost_microusd,
                    "cost_usd": run.metrics.cost_microusd as f64 / 1_000_000.0,
                    "duration_ms": run.metrics.duration_ms,
                    "unpriced": run.unpriced,
                })
            })
            .collect::<Vec<_>>();
        let total_microusd = runs
            .iter()
            .map(|run| run.metrics.cost_microusd)
            .fold(0_u64, u64::saturating_add);
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "runs": items,
                "total_cost_microusd": total_microusd,
                "total_cost_usd": total_microusd as f64 / 1_000_000.0,
            }))?
        );
        return Ok(());
    }
    println!("run\tstate\tgenerator\ttokens(in/out/cached)\tcalls\tcost\tunpriced");
    for run in &runs {
        println!(
            "{}\t{}\t{}\t{}/{}/{}\t{}\t{}\t{}",
            run.run_id,
            run.state,
            run.generator,
            run.metrics.tokens_input,
            run.metrics.tokens_output,
            run.metrics.tokens_cached_input,
            run.metrics.llm_calls,
            format_usd(run.metrics.cost_microusd),
            if run.unpriced { "yes" } else { "no" },
        );
    }
    let total_microusd = runs
        .iter()
        .map(|run| run.metrics.cost_microusd)
        .fold(0_u64, u64::saturating_add);
    println!(
        "total\t\t\t\t\t\t{}\t{total_microusd} microUSD",
        format_usd(total_microusd),
    );
    if by_model && !models.is_empty() {
        println!(
            "model\tprovider\ttokens(in/out/cached)\tcalls\tunit(in/out USD per 1M)\tcost\tunpriced_calls"
        );
        let mut models: Vec<&ModelAggregate> = models.values().collect();
        models.sort_by(|left, right| {
            (left.provider.clone(), left.model.clone())
                .cmp(&(right.provider.clone(), right.model.clone()))
        });
        for entry in models {
            let rates = match (entry.input_rate, entry.output_rate) {
                (Some(input), Some(output)) => format!("{input}/{output}"),
                _ => "n/a".into(),
            };
            println!(
                "{}\t{}\t{}/{}/{}\t{}\t{}\t{}\t{}",
                entry.model,
                entry.provider,
                entry.tokens_input,
                entry.tokens_output,
                entry.tokens_cached_input,
                entry.calls,
                rates,
                format_usd(entry.cost_microusd),
                entry.unpriced_calls,
            );
        }
    }
    Ok(())
}

/// Contract unit prices (USD per 1M tokens) for one provider/model route,
/// preferring a fully priced entry like the execution gateway.
pub(crate) fn find_pricing(contract: &Contract, provider: &str, model: &str) -> Option<(f64, f64)> {
    let candidates = contract
        .manifest
        .llm
        .as_ref()
        .map(|llm| {
            llm.models
                .iter()
                .chain(llm.model.as_ref())
                .filter(|entry| entry.provider == provider && entry.model == model)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    candidates.iter().find_map(|entry| {
        match (
            entry.input_cost_per_million_usd,
            entry.output_cost_per_million_usd,
        ) {
            (Some(input), Some(output)) => Some((input, output)),
            _ => None,
        }
    })
}
