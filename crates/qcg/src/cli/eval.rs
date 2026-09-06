use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{Contract, RuntimeLimits};
use qcg_policy::validate_bounded_json_schema;
use qcg_service::{DirectRun, LocalQcgService};
use qcg_types::OutputManifest;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::artifacts::{declared_artifact, read_declared_artifact};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvalSuite {
    pub(crate) name: String,
    #[serde(default = "default_min_pass_rate")]
    pub(crate) min_pass_rate: f64,
    #[serde(default = "default_eval_repetitions")]
    pub(crate) repetitions: usize,
    pub(crate) cases: Vec<EvalCase>,
}

fn default_min_pass_rate() -> f64 {
    1.0
}

fn default_eval_repetitions() -> usize {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvalCase {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub(crate) answers: BTreeMap<String, Value>,
    #[serde(default)]
    pub(crate) confirmations: BTreeMap<String, bool>,
    #[serde(default)]
    pub(crate) seed: Option<u64>,
    pub(crate) assertions: Vec<EvalAssertion>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum EvalAssertion {
    ArtifactExists {
        path: String,
    },
    ArtifactSha256 {
        path: String,
        sha256: String,
    },
    ArtifactContains {
        path: String,
        text: String,
    },
    ManifestPointer {
        pointer: String,
        equals: Value,
    },
    EventCount {
        kind: String,
        #[serde(default)]
        min: Option<usize>,
        #[serde(default)]
        max: Option<usize>,
    },
    ArtifactMatches {
        path: String,
        pattern: String,
    },
    ArtifactJsonSchema {
        path: String,
        schema: Value,
    },
    EventSequence {
        kinds: Vec<String>,
    },
    MetricMax {
        metric: String,
        max: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EvalReport {
    suite: String,
    generator: String,
    passed: usize,
    total: usize,
    pass_rate: f64,
    min_pass_rate: f64,
    repetitions: usize,
    #[serde(default)]
    baseline: Option<EvalBaselineComparison>,
    #[serde(default)]
    case_summary: Vec<EvalCaseSummary>,
    #[serde(default)]
    flaky_cases: Vec<String>,
    cases: Vec<EvalCaseReport>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EvalCaseSummary {
    pub(crate) name: String,
    pub(crate) passed: usize,
    pub(crate) total: usize,
    pub(crate) flaky: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EvalCaseReport {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) repetition: usize,
    pub(crate) passed: bool,
    pub(crate) failures: Vec<String>,
    #[serde(default)]
    pub(crate) tokens_total: u64,
    #[serde(default)]
    pub(crate) cost_microusd: u64,
    #[serde(default)]
    pub(crate) duration_ms: u64,
    #[serde(default)]
    pub(crate) llm_calls: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EvalBaselineComparison {
    baseline_pass_rate: f64,
    pass_rate_delta: f64,
    regressed_cases: Vec<String>,
    #[serde(default)]
    baseline_cost_microusd_p50: u64,
    #[serde(default)]
    cost_microusd_p50_delta: i64,
    #[serde(default)]
    baseline_duration_ms_p50: u64,
    #[serde(default)]
    duration_ms_p50_delta: i64,
}

pub(crate) async fn run_eval(
    generator: Utf8PathBuf,
    suite_path: &Utf8Path,
    output_root: &Utf8Path,
    runs_dir: Utf8PathBuf,
    providers_path: Option<Utf8PathBuf>,
    baseline_path: Option<&Utf8Path>,
    json_output: bool,
) -> Result<()> {
    let source = qcg_policy::read_bounded(suite_path, None)?;
    let suite: EvalSuite = serde_json::from_slice(&source)
        .with_context(|| format!("invalid eval suite `{suite_path}`"))?;
    validate_eval_suite(&suite)?;
    let generator_contract = Contract::load(&generator)
        .with_context(|| format!("failed to load eval generator `{generator}`"))?;
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%S%.fZ").to_string();
    let eval_root = output_root.join(&suite.name).join(timestamp);
    std::fs::create_dir_all(&eval_root)?;
    let service = LocalQcgService::new(Utf8PathBuf::new(), runs_dir, providers_path)?;
    let mut reports = Vec::with_capacity(suite.cases.len() * suite.repetitions);
    for repetition in 0..suite.repetitions {
        for (index, case) in suite.cases.iter().enumerate() {
            let case_root = eval_root.join(format!("{index:04}-{}-run-{repetition:03}", case.name));
            let run = DirectRun {
                generator_path: generator.clone(),
                inputs: case.inputs.clone(),
                output_dir: case_root.clone(),
                json_events: false,
                interactive: false,
                answers: case.answers.clone(),
                confirmations: case.confirmations.clone(),
                llm_seed_override: case.seed.map(|seed| seed.saturating_add(repetition as u64)),
            };
            let report = match service.run_generator_path_with_events(run).await {
                Ok(result) => {
                    let metrics = eval_terminal_metrics(&result.events);
                    EvalCaseReport {
                        name: case.name.clone(),
                        repetition,
                        failures: evaluate_assertions(
                            &case.assertions,
                            &case_root,
                            &result.manifest,
                            &generator_contract.manifest.runtime,
                            &result.events,
                        )?,
                        passed: false,
                        tokens_total: metrics.0,
                        cost_microusd: metrics.1,
                        duration_ms: metrics.2,
                        llm_calls: metrics.3,
                    }
                }
                Err(error) => EvalCaseReport {
                    name: case.name.clone(),
                    repetition,
                    passed: false,
                    failures: vec![format!("run failed: {error}")],
                    tokens_total: 0,
                    cost_microusd: 0,
                    duration_ms: 0,
                    llm_calls: 0,
                },
            };
            reports.push(EvalCaseReport {
                passed: report.failures.is_empty(),
                ..report
            });
        }
    }
    let passed = reports.iter().filter(|case| case.passed).count();
    let total = reports.len();
    let pass_rate = if total == 0 {
        0.0
    } else {
        passed as f64 / total as f64
    };
    let (case_summary, flaky_cases) = summarize_eval_cases(&suite, &reports);
    let baseline = baseline_path
        .map(|path| compare_eval_baseline(path, pass_rate, &reports))
        .transpose()?;
    let report = EvalReport {
        suite: suite.name,
        generator: generator.to_string(),
        passed,
        total,
        pass_rate,
        min_pass_rate: suite.min_pass_rate,
        repetitions: suite.repetitions,
        baseline,
        case_summary,
        flaky_cases: flaky_cases.clone(),
        cases: reports,
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    std::fs::write(eval_root.join("report.json"), &encoded)?;
    if json_output {
        println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    } else {
        println!(
            "eval {}: {}/{} passed ({:.2}%), report {}",
            report.suite,
            report.passed,
            report.total,
            report.pass_rate * 100.0,
            eval_root.join("report.json")
        );
        for case in &report.cases {
            if !case.passed {
                for failure in &case.failures {
                    eprintln!("{}: {failure}", case.name);
                }
            }
        }
        if !report.flaky_cases.is_empty() {
            eprintln!("flaky: {}", report.flaky_cases.join(", "));
        }
    }
    if report.pass_rate < report.min_pass_rate {
        anyhow::bail!(
            "eval pass rate {:.4} is below required {:.4}",
            report.pass_rate,
            report.min_pass_rate
        );
    }
    if let Some(comparison) = &report.baseline
        && (!comparison.regressed_cases.is_empty() || comparison.pass_rate_delta < 0.0)
    {
        anyhow::bail!(
            "eval regressed from baseline by {:.4}; regressed cases: {}",
            comparison.pass_rate_delta,
            comparison.regressed_cases.join(", ")
        );
    }
    Ok(())
}

fn validate_eval_suite(suite: &EvalSuite) -> Result<()> {
    if !safe_eval_name(&suite.name) || suite.cases.is_empty() {
        anyhow::bail!("eval suite requires a safe name and at least one case");
    }
    if !suite.min_pass_rate.is_finite() || !(0.0..=1.0).contains(&suite.min_pass_rate) {
        anyhow::bail!("eval suite min_pass_rate must be from 0 through 1");
    }
    if suite.repetitions == 0 || suite.repetitions > 100 {
        anyhow::bail!("eval suite repetitions must be from 1 through 100");
    }
    let mut names = BTreeSet::new();
    for case in &suite.cases {
        if !safe_eval_name(&case.name) || !names.insert(case.name.as_str()) {
            anyhow::bail!("eval case names must be safe and unique");
        }
        if case.assertions.is_empty() {
            anyhow::bail!("eval case `{}` requires at least one assertion", case.name);
        }
        for assertion in &case.assertions {
            match assertion {
                EvalAssertion::ArtifactMatches { pattern, .. } => {
                    regex::Regex::new(pattern).with_context(|| {
                        format!("eval case `{}` has invalid regex `{pattern}`", case.name)
                    })?;
                }
                EvalAssertion::ArtifactJsonSchema { schema, .. } => {
                    validate_bounded_json_schema(schema).map_err(|error| {
                        anyhow::anyhow!(
                            "eval case `{}` has invalid or unsafe JSON Schema: {error}",
                            case.name
                        )
                    })?;
                    qcg_policy::compile_bounded_validator(schema).map_err(|error| {
                        anyhow::anyhow!(
                            "eval case `{}` has invalid JSON Schema: {error}",
                            case.name
                        )
                    })?;
                }
                EvalAssertion::EventSequence { kinds } if kinds.is_empty() => {
                    anyhow::bail!(
                        "eval case `{}` event_sequence requires at least one kind",
                        case.name
                    );
                }
                EvalAssertion::MetricMax { metric, .. } if metric.trim().is_empty() => {
                    anyhow::bail!("eval case `{}` metric_max requires a metric", case.name);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn safe_eval_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 80
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn compare_eval_baseline(
    path: &Utf8Path,
    pass_rate: f64,
    reports: &[EvalCaseReport],
) -> Result<EvalBaselineComparison> {
    let baseline: EvalReport = serde_json::from_slice(&qcg_policy::read_bounded(path, None)?)
        .with_context(|| format!("invalid eval baseline `{path}`"))?;
    let baseline_passed = baseline
        .cases
        .iter()
        .map(|case| ((case.name.as_str(), case.repetition), case.passed))
        .collect::<BTreeMap<_, _>>();
    let mut regressed_cases = reports
        .iter()
        .filter(|case| {
            baseline_passed
                .get(&(case.name.as_str(), case.repetition))
                .copied()
                == Some(true)
                && !case.passed
        })
        .map(|case| format!("{}#{}", case.name, case.repetition))
        .collect::<Vec<_>>();
    regressed_cases.sort();
    let baseline_cost_p50 = percentile_u64(
        &baseline
            .cases
            .iter()
            .map(|case| case.cost_microusd)
            .collect::<Vec<_>>(),
    );
    let current_cost_p50 = percentile_u64(
        &reports
            .iter()
            .map(|case| case.cost_microusd)
            .collect::<Vec<_>>(),
    );
    let baseline_duration_p50 = percentile_u64(
        &baseline
            .cases
            .iter()
            .map(|case| case.duration_ms)
            .collect::<Vec<_>>(),
    );
    let current_duration_p50 = percentile_u64(
        &reports
            .iter()
            .map(|case| case.duration_ms)
            .collect::<Vec<_>>(),
    );
    Ok(EvalBaselineComparison {
        baseline_pass_rate: baseline.pass_rate,
        pass_rate_delta: pass_rate - baseline.pass_rate,
        regressed_cases,
        baseline_cost_microusd_p50: baseline_cost_p50,
        cost_microusd_p50_delta: current_cost_p50 as i64 - baseline_cost_p50 as i64,
        baseline_duration_ms_p50: baseline_duration_p50,
        duration_ms_p50_delta: current_duration_p50 as i64 - baseline_duration_p50 as i64,
    })
}

/// Median (p50) of a value list; empty input yields 0.
pub(crate) fn percentile_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

/// Terminal metrics from run events when the run finished.
fn eval_terminal_metrics(events: &[qcg_api::RunEvent]) -> (u64, u64, u64, u64) {
    use qcg_api::RunEventData;
    for event in events.iter().rev() {
        match &event.data {
            RunEventData::RunFinished(data) => {
                let metrics = &data.metrics;
                return (
                    metrics.tokens_input.saturating_add(metrics.tokens_output),
                    metrics.cost_microusd,
                    metrics.duration_ms,
                    metrics.llm_calls,
                );
            }
            RunEventData::RunError(data) => {
                let metrics = &data.metrics;
                return (
                    metrics.tokens_input.saturating_add(metrics.tokens_output),
                    metrics.cost_microusd,
                    metrics.duration_ms,
                    metrics.llm_calls,
                );
            }
            _ => {}
        }
    }
    (0, 0, 0, 0)
}

/// Aggregate per-case pass counts across repetitions; a case passing at least
/// once and failing at least once is reported as flaky.
pub(crate) fn summarize_eval_cases(
    suite: &EvalSuite,
    reports: &[EvalCaseReport],
) -> (Vec<EvalCaseSummary>, Vec<String>) {
    let mut summary = Vec::new();
    let mut flaky = Vec::new();
    for case in &suite.cases {
        let total = reports
            .iter()
            .filter(|report| report.name == case.name)
            .count();
        let passed = reports
            .iter()
            .filter(|report| report.name == case.name && report.passed)
            .count();
        let is_flaky = passed > 0 && passed < total;
        if is_flaky {
            flaky.push(case.name.clone());
        }
        summary.push(EvalCaseSummary {
            name: case.name.clone(),
            passed,
            total,
            flaky: is_flaky,
        });
    }
    (summary, flaky)
}

fn evaluate_assertions(
    assertions: &[EvalAssertion],
    output_root: &Utf8Path,
    manifest: &OutputManifest,
    runtime: &RuntimeLimits,
    events: &[qcg_api::RunEvent],
) -> Result<Vec<String>> {
    let manifest_value = serde_json::to_value(manifest)?;
    let mut failures = Vec::new();
    for assertion in assertions {
        let failure = match assertion {
            EvalAssertion::ArtifactExists { path } => declared_artifact(manifest, path).err(),
            EvalAssertion::ArtifactSha256 { path, sha256 } => {
                match declared_artifact(manifest, path) {
                    Ok(artifact) => (artifact.sha256 != *sha256).then(|| {
                        format!(
                            "artifact `{path}` sha256 was {}, expected {sha256}",
                            artifact.sha256
                        )
                    }),
                    Err(error) => Some(error),
                }
            }
            EvalAssertion::ArtifactContains { path, text } => {
                match read_declared_artifact(output_root, manifest, path, runtime) {
                    Ok(bytes) => match std::str::from_utf8(&bytes) {
                        Ok(content) if content.contains(text) => None,
                        Ok(_) => Some(format!("artifact `{path}` did not contain expected text")),
                        Err(error) => Some(format!("artifact `{path}` is not UTF-8: {error}")),
                    },
                    Err(error) => Some(error),
                }
            }
            EvalAssertion::ManifestPointer { pointer, equals } => manifest_value
                .pointer(pointer)
                .filter(|actual| *actual == equals)
                .is_none()
                .then(|| format!("manifest pointer `{pointer}` did not equal {equals}")),
            EvalAssertion::EventCount { kind, min, max } => {
                let count = events.iter().filter(|event| event.kind == *kind).count();
                let below = min.is_some_and(|minimum| count < minimum);
                let above = max.is_some_and(|maximum| count > maximum);
                (below || above).then(|| {
                    format!(
                        "event `{kind}` count {count} was outside {}..{}",
                        min.unwrap_or_default(),
                        max.map_or_else(|| "unbounded".into(), |value| value.to_string())
                    )
                })
            }
            EvalAssertion::ArtifactMatches { path, pattern } => {
                let regex = regex::Regex::new(pattern)
                    .with_context(|| format!("invalid artifact regex `{pattern}`"))?;
                match read_declared_artifact(output_root, manifest, path, runtime) {
                    Ok(bytes) => match std::str::from_utf8(&bytes) {
                        Ok(content) if regex.is_match(content) => None,
                        Ok(_) => Some(format!("artifact `{path}` did not match `{pattern}`")),
                        Err(error) => Some(format!("artifact `{path}` is not UTF-8: {error}")),
                    },
                    Err(error) => Some(error),
                }
            }
            EvalAssertion::ArtifactJsonSchema { path, schema } => {
                let validator = qcg_policy::compile_bounded_validator(schema).map_err(|error| {
                    anyhow::anyhow!("invalid JSON Schema for artifact `{path}`: {error}")
                })?;
                match read_declared_artifact(output_root, manifest, path, runtime).and_then(
                    |bytes| {
                        serde_json::from_slice::<Value>(&bytes).map_err(|error| error.to_string())
                    },
                ) {
                    Ok(value) => validator.validate(&value).err().map(|error| {
                        format!(
                            "artifact `{path}` failed JSON Schema at `{}`",
                            error.instance_path()
                        )
                    }),
                    Err(error) => Some(format!(
                        "artifact `{path}` could not be parsed as JSON: {error}"
                    )),
                }
            }
            EvalAssertion::EventSequence { kinds } => {
                let mut expected = kinds.iter();
                let mut next = expected.next();
                for event in events {
                    if next.is_some_and(|kind| kind == &event.kind) {
                        next = expected.next();
                    }
                }
                next.map(|missing| {
                    format!("event trajectory did not reach ordered event `{missing}`")
                })
            }
            EvalAssertion::MetricMax { metric, max } => {
                let pointer = format!("/metrics/{metric}");
                let actual = events
                    .iter()
                    .rev()
                    .find(|event| event.kind == "run_finished")
                    .and_then(|event| serde_json::to_value(&event.data).ok())
                    .and_then(|data| data.pointer(&pointer).and_then(Value::as_u64));
                match actual {
                    Some(actual) if actual <= *max => None,
                    Some(actual) => Some(format!(
                        "metric `{metric}` was {actual}, exceeding maximum {max}"
                    )),
                    None => Some(format!("metric `{metric}` was not recorded")),
                }
            }
        };
        if let Some(failure) = failure {
            failures.push(failure);
        }
    }
    Ok(failures)
}
