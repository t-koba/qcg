use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand, ValueEnum};
use futures_util::StreamExt;
use qcg_api::{ForkRun, ForkStatePatch, RunStatus};
use qcg_contract::{Contract, RuntimeLimits, validate_bounded_json_schema};
use qcg_engine::{OutputArtifact, OutputManifest, read_output_manifest};
use qcg_service::{
    DirectRun, LocalQcgService, list_run_summaries, read_run_events, read_run_generator_path,
    read_run_inputs, resolve_run_dir, run_meta_dir, run_summary, step_param_schemas_markdown,
};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead as _, ErrorKind, Read as _, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

const MAX_CLI_JSON_INPUT_BYTES: usize = qcg_types::MAX_FILE_INPUT_BYTES * 2;
const MAX_PACKAGE_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_SIGNING_KEY_BYTES: usize = 64 * 1024;
const MAX_PACKAGE_ENTRIES: usize = 10_000;
const MAX_PACKAGE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PACKAGE_ARCHIVE_BYTES: usize = MAX_PACKAGE_BYTES as usize;
const GENERATED_PACKAGE_ENTRIES: usize = 2;
const MAX_PACKAGE_SOURCE_ENTRIES: usize = MAX_PACKAGE_ENTRIES - GENERATED_PACKAGE_ENTRIES;
const MAX_CLI_DIRECTORY_SCAN_ENTRIES: usize = 100_000;
const MAX_CONFIRM_INPUT_BYTES: usize = 1024;

#[derive(Debug, Parser)]
#[command(
    name = "qcg",
    version,
    about = "Contract-driven harness for bounded, purpose-specialized generation"
)]
struct Cli {
    #[arg(long, global = true)]
    verbose: bool,
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "QCG_PROVIDERS",
        help = "Path to the LLM, search, and MCP providers registry; defaults to ./providers.toml"
    )]
    providers: Option<Utf8PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        path: Utf8PathBuf,
    },
    Run {
        generator: Utf8PathBuf,
        #[arg(long = "input")]
        inputs: Vec<String>,
        #[arg(long = "inputs-file")]
        inputs_file: Option<Utf8PathBuf>,
        #[arg(long = "input-file", value_name = "FIELD=PATH")]
        input_files: Vec<String>,
        #[arg(long = "answer")]
        answers: Vec<String>,
        #[arg(long = "output", default_value = "out")]
        output: Utf8PathBuf,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        json: bool,
    },
    Eval {
        generator: Utf8PathBuf,
        #[arg(long)]
        suite: Utf8PathBuf,
        #[arg(long = "output", default_value = ".qcg/evals")]
        output: Utf8PathBuf,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long, value_name = "REPORT_JSON")]
        baseline: Option<Utf8PathBuf>,
        #[arg(long)]
        json: bool,
    },
    List {
        #[arg(
            long = "generators-dir",
            env = "QCG_GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
    },
    Runs {
        #[command(subcommand)]
        command: RunsCommand,
    },
    Docs {
        #[command(subcommand)]
        command: DocsCommand,
    },
    Package {
        dir: Utf8PathBuf,
        #[arg(short, long)]
        output: Option<Utf8PathBuf>,
        #[arg(long = "signing-key", value_name = "PKCS8_PATH")]
        signing_key: Option<Utf8PathBuf>,
    },
    Install {
        source: String,
        #[arg(
            long = "generators-dir",
            env = "QCG_GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        force: bool,
        #[arg(long, value_name = "HEX")]
        sha256: Option<String>,
        #[arg(long, value_name = "HEX")]
        signature: Option<String>,
        #[arg(long = "public-key", value_name = "HEX")]
        public_key: Option<String>,
    },
    Uninstall {
        id: String,
        #[arg(
            long = "generators-dir",
            env = "QCG_GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long)]
        yes: bool,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 0)]
        port: u16,
        #[arg(
            long = "generators-dir",
            env = "QCG_GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(
            long,
            env = "QCG_MAX_ACTIVE_RUNS",
            default_value_t = qcg_service::DEFAULT_MAX_ACTIVE_RUNS
        )]
        max_active_runs: usize,
        #[arg(
            long,
            env = "QCG_MAX_TRACKED_RUNS",
            default_value_t = qcg_service::DEFAULT_MAX_TRACKED_RUNS
        )]
        max_tracked_runs: usize,
        #[arg(long, env = "QCG_RUN_STORE", value_enum, default_value_t = RunStoreArg::Exclusive)]
        run_store: RunStoreArg,
        #[arg(
            long = "cors-origin",
            env = "QCG_CORS_ORIGIN",
            value_delimiter = ',',
            value_name = "ORIGIN"
        )]
        cors_origins: Vec<String>,
        #[arg(long, env = "QCG_API_TOKEN", hide_env_values = true)]
        api_token: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RunStoreArg {
    Exclusive,
    #[value(name = "shared-filesystem")]
    SharedFilesystem,
}

impl From<RunStoreArg> for qcg_service::RunStoreMode {
    fn from(value: RunStoreArg) -> Self {
        match value {
            RunStoreArg::Exclusive => Self::Exclusive,
            RunStoreArg::SharedFilesystem => Self::SharedFilesystem,
        }
    }
}

#[derive(Debug, Subcommand)]
enum RunsCommand {
    List {
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
    },
    Show {
        id: String,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
    },
    Replay {
        id: String,
        generator: Option<Utf8PathBuf>,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long = "output")]
        output: Option<Utf8PathBuf>,
        #[arg(long = "reuse-seed")]
        reuse_seed: bool,
        #[arg(long)]
        json: bool,
    },
    Fork {
        id: String,
        #[arg(long = "at-seq")]
        at_seq: u64,
        #[arg(long = "state-patch", value_name = "JSON_FILE")]
        state_patch: Option<Utf8PathBuf>,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
    },
    Trace {
        id: String,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long, value_name = "OTLP_JSON")]
        output: Option<Utf8PathBuf>,
        #[arg(long = "otlp-endpoint", value_name = "URL")]
        otlp_endpoint: Option<String>,
    },
    Gc {
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long, default_value_t = 50)]
        keep: usize,
        #[arg(long)]
        delete: bool,
    },
}

#[derive(Debug, Subcommand)]
enum DocsCommand {
    StepSchemas,
    RunEvents,
    Openapi,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose, cli.log_format);
    let providers_path = cli.providers.clone();
    match cli.command {
        Command::Validate { path } => {
            let contract = Contract::load(&path)?;
            app_registry(providers_path.as_deref())?.validate_contract(&contract)?;
            println!(
                "valid: {}@{} ({})",
                contract.manifest.generator.id,
                contract.manifest.generator.version,
                contract.sha256
            );
        }
        Command::Run {
            generator,
            inputs,
            inputs_file,
            input_files,
            answers,
            output,
            yes,
            json,
        } => {
            let input_runtime = Contract::load(&generator)?.manifest.runtime;
            let inputs = load_inputs(inputs, inputs_file, input_files, &input_runtime)?;
            let answers = load_answers(answers)?;
            let runs_dir = Utf8PathBuf::from(".qcg/runs");
            let service =
                LocalQcgService::new(Utf8PathBuf::new(), runs_dir.clone(), providers_path)?;
            auto_gc_runs(&runs_dir)?;
            let run = DirectRun {
                generator_path: generator,
                inputs,
                output_dir: output,
                json_events: false,
                interactive: !yes,
                answers,
                confirmations: BTreeMap::new(),
                llm_seed_override: None,
            };
            if json {
                let result = service.run_generator_path_with_events(run).await?;
                for event in result.events {
                    println!("{}", serde_json::to_string(&event)?);
                }
            } else {
                let manifest = service.run_generator_path(run).await?;
                println!("{}", serde_json::to_string_pretty(&manifest)?);
            }
        }
        Command::Eval {
            generator,
            suite,
            output,
            runs_dir,
            baseline,
            json,
        } => {
            run_eval(
                generator,
                &suite,
                &output,
                runs_dir,
                providers_path,
                baseline.as_deref(),
                json,
            )
            .await?;
        }
        Command::List { generators_dir } => {
            let mut roots = vec![generators_dir.clone()];
            if let Some(bundled) = bundled_generators_root()
                && bundled != generators_dir
            {
                roots.push(bundled);
            }
            let mut seen = BTreeSet::new();
            let mut scanned = 0_usize;
            for root in &roots {
                if !root.exists() {
                    continue;
                }
                for entry in std::fs::read_dir(root)? {
                    scanned = scanned.saturating_add(1);
                    if scanned > MAX_CLI_DIRECTORY_SCAN_ENTRIES {
                        anyhow::bail!(
                            "generator roots contain more than {MAX_CLI_DIRECTORY_SCAN_ENTRIES} entries"
                        );
                    }
                    let entry = entry?;
                    let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                        anyhow::anyhow!("path is not valid UTF-8: {}", path.display())
                    })?;
                    if path.join("qcg.toml").exists() {
                        match Contract::load(&path) {
                            Ok(contract) if seen.insert(contract.manifest.generator.id.clone()) => {
                                println!(
                                    "{}\t{}\t{}",
                                    contract.manifest.generator.id,
                                    contract.manifest.generator.version,
                                    contract.manifest.generator.name
                                );
                            }
                            Ok(_) => {}
                            Err(error) => eprintln!("invalid generator `{path}`: {error}"),
                        }
                    }
                }
            }
        }
        Command::Docs { command } => match command {
            DocsCommand::StepSchemas => {
                print!("{}", step_param_schemas_markdown()?);
            }
            DocsCommand::RunEvents => {
                print!("{}", qcg_api::run_event_reference_markdown());
            }
            DocsCommand::Openapi => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&qcg_api::openapi_document(env!(
                        "CARGO_PKG_VERSION"
                    )))?
                );
            }
        },
        Command::Runs { command } => match command {
            RunsCommand::List { runs_dir } => list_runs(&runs_dir)?,
            RunsCommand::Show { id, runs_dir, json } => show_run(&runs_dir, &id, json)?,
            RunsCommand::Replay {
                id,
                generator,
                runs_dir,
                output,
                reuse_seed,
                json,
            } => {
                replay_run(
                    &runs_dir,
                    &id,
                    &generator,
                    output,
                    reuse_seed,
                    json,
                    providers_path,
                )
                .await?
            }
            RunsCommand::Fork {
                id,
                at_seq,
                state_patch,
                runs_dir,
                json,
            } => {
                let state_patch = match state_patch {
                    Some(path) => serde_json::from_slice::<ForkStatePatch>(&read_file_bounded(
                        &path,
                        MAX_CLI_JSON_INPUT_BYTES,
                    )?)
                    .with_context(|| format!("failed to parse state patch `{path}`"))?,
                    None => ForkStatePatch::default(),
                };
                let service = LocalQcgService::new(Utf8PathBuf::new(), runs_dir, providers_path)?;
                let fork_id = service
                    .fork_run(
                        &id,
                        ForkRun {
                            at_seq,
                            state_patch,
                        },
                    )
                    .await?;
                let snapshot = loop {
                    let snapshot = service.snapshot(fork_id.clone()).await?;
                    if !matches!(snapshot.state, RunStatus::Queued | RunStatus::Running) {
                        break snapshot;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                };
                if json {
                    println!("{}", serde_json::to_string_pretty(&snapshot)?);
                } else {
                    println!("forked {id}@{at_seq} -> {fork_id} ({})", snapshot.state);
                }
            }
            RunsCommand::Trace {
                id,
                runs_dir,
                output,
                otlp_endpoint,
            } => {
                export_run_trace(&runs_dir, &id, output.as_deref(), otlp_endpoint.as_deref())
                    .await?
            }
            RunsCommand::Gc {
                runs_dir,
                keep,
                delete,
            } => gc_runs(&runs_dir, keep, delete)?,
        },
        Command::Package {
            dir,
            output,
            signing_key,
        } => {
            let output = output.unwrap_or_else(|| {
                Utf8PathBuf::from(format!("{}.qcg", dir.file_name().unwrap_or("generator")))
            });
            package(&dir, &output)?;
            println!("sha256 {}", sha256_file(&output)?);
            if let Some(signing_key) = signing_key {
                let bytes = read_file_bounded(&output, MAX_PACKAGE_ARCHIVE_BYTES)?;
                sign_package(&output, &bytes, &signing_key)?;
            }
            println!("packaged {output}");
        }
        Command::Install {
            source,
            generators_dir,
            yes,
            force,
            sha256,
            signature,
            public_key,
        } => {
            let installed = install(
                providers_path.as_deref(),
                &source,
                &generators_dir,
                yes,
                force,
                InstallVerification {
                    sha256: sha256.as_deref(),
                    signature: signature.as_deref(),
                    public_key: public_key.as_deref(),
                },
            )
            .await?;
            println!("installed {}", installed);
        }
        Command::Uninstall {
            id,
            generators_dir,
            yes,
        } => {
            uninstall(&id, &generators_dir, yes)?;
            println!("uninstalled {id}");
        }
        Command::Serve {
            bind,
            port,
            generators_dir,
            runs_dir,
            max_active_runs,
            max_tracked_runs,
            run_store,
            cors_origins,
            api_token,
        } => {
            let addr: std::net::SocketAddr = format!("{bind}:{port}").parse()?;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let actual_addr = listener.local_addr()?;
            println!("qcg server listening on http://{actual_addr}");
            qcg_server::serve_with_listener(
                qcg_server::ServerConfig {
                    providers_path,
                    extra_generators_dirs: bundled_generators_root()
                        .filter(|bundled| bundled != &generators_dir)
                        .into_iter()
                        .collect(),
                    generators_dir,
                    runs_dir,
                    max_active_runs,
                    max_tracked_runs,
                    run_store_mode: run_store.into(),
                    cors_origins,
                    api_token,
                },
                listener,
            )
            .await?;
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalSuite {
    name: String,
    #[serde(default = "default_min_pass_rate")]
    min_pass_rate: f64,
    #[serde(default = "default_eval_repetitions")]
    repetitions: usize,
    cases: Vec<EvalCase>,
}

fn default_min_pass_rate() -> f64 {
    1.0
}

fn default_eval_repetitions() -> usize {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalCase {
    name: String,
    #[serde(default)]
    inputs: BTreeMap<String, Value>,
    #[serde(default)]
    answers: BTreeMap<String, Value>,
    #[serde(default)]
    confirmations: BTreeMap<String, bool>,
    #[serde(default)]
    seed: Option<u64>,
    assertions: Vec<EvalAssertion>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EvalAssertion {
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
struct EvalReport {
    suite: String,
    generator: String,
    passed: usize,
    total: usize,
    pass_rate: f64,
    min_pass_rate: f64,
    repetitions: usize,
    #[serde(default)]
    baseline: Option<EvalBaselineComparison>,
    cases: Vec<EvalCaseReport>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EvalCaseReport {
    name: String,
    #[serde(default)]
    repetition: usize,
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EvalBaselineComparison {
    baseline_pass_rate: f64,
    pass_rate_delta: f64,
    regressed_cases: Vec<String>,
}

async fn run_eval(
    generator: Utf8PathBuf,
    suite_path: &Utf8Path,
    output_root: &Utf8Path,
    runs_dir: Utf8PathBuf,
    providers_path: Option<Utf8PathBuf>,
    baseline_path: Option<&Utf8Path>,
    json_output: bool,
) -> Result<()> {
    let source = read_file_bounded(suite_path, MAX_CLI_JSON_INPUT_BYTES)?;
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
                Ok(result) => EvalCaseReport {
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
                },
                Err(error) => EvalCaseReport {
                    name: case.name.clone(),
                    repetition,
                    passed: false,
                    failures: vec![format!("run failed: {error}")],
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
                    jsonschema::validator_for(schema).with_context(|| {
                        format!("eval case `{}` has invalid JSON Schema", case.name)
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
    let baseline: EvalReport =
        serde_json::from_slice(&read_file_bounded(path, MAX_CLI_JSON_INPUT_BYTES)?)
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
    Ok(EvalBaselineComparison {
        baseline_pass_rate: baseline.pass_rate,
        pass_rate_delta: pass_rate - baseline.pass_rate,
        regressed_cases,
    })
}

fn evaluate_assertions(
    assertions: &[EvalAssertion],
    output_root: &Utf8Path,
    manifest: &OutputManifest,
    runtime: &RuntimeLimits,
    events: &[qcg_types::RunEvent],
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
                let validator = jsonschema::validator_for(schema)
                    .with_context(|| format!("invalid JSON Schema for artifact `{path}`"))?;
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

fn declared_artifact<'a>(
    manifest: &'a OutputManifest,
    path: &str,
) -> Result<&'a OutputArtifact, String> {
    if !qcg_types::is_safe_relative_path(path) {
        return Err(format!("artifact assertion path `{path}` is unsafe"));
    }
    manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.path == path)
        .ok_or_else(|| format!("artifact `{path}` does not exist"))
}

fn read_declared_artifact(
    output_root: &Utf8Path,
    manifest: &OutputManifest,
    path: &str,
    runtime: &RuntimeLimits,
) -> Result<Vec<u8>, String> {
    let artifact = declared_artifact(manifest, path)?;
    let declared = usize::try_from(artifact.bytes)
        .map_err(|_| format!("artifact `{path}` byte count does not fit this platform"))?;
    if artifact.bytes > runtime.output_file_limit_bytes as u64 {
        return Err(format!(
            "artifact `{path}` exceeds runtime.output_file_limit_bytes ({})",
            runtime.output_file_limit_bytes
        ));
    }
    let bytes = read_file_bounded(&output_root.join(&artifact.path), declared)
        .map_err(|error| format!("artifact `{path}` could not be read: {error}"))?;
    if bytes.len() != declared {
        return Err(format!(
            "artifact `{path}` size changed: manifest declares {declared} bytes, found {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

/// Read-only generator catalog bundled next to the binary
/// (`<prefix>/share/qcg/generators`). Searched after the user's root so
/// installed packages shadow bundled demos with the same id.
fn bundled_generators_root() -> Option<Utf8PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let bin_dir = exe.parent()?;
    let prefix = bin_dir.parent()?;
    let root = Utf8PathBuf::from_path_buf(prefix.join("share/qcg/generators")).ok()?;
    root.is_dir().then_some(root)
}

fn init_tracing(verbose: bool, format: LogFormat) {
    let default_filter = if verbose { "qcg=debug" } else { "qcg=info" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    match format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init(),
    }
}

fn list_runs(runs_dir: &Utf8Path) -> Result<()> {
    for summary in list_run_summaries(runs_dir)? {
        println!(
            "{}\t{}\t{}\t{}",
            summary.run_id, summary.status, summary.generator, summary.started_at
        );
    }
    Ok(())
}

fn show_run(runs_dir: &Utf8Path, id: &str, json_output: bool) -> Result<()> {
    let run_dir = resolve_run_dir(runs_dir, id)?;
    let summary = run_summary(&run_dir)?;
    if json_output {
        println!("{}", serde_json::to_string_pretty(&summary.to_json())?);
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
    Ok(())
}

async fn replay_run(
    runs_dir: &Utf8Path,
    id: &str,
    generator: &Option<Utf8PathBuf>,
    output: Option<Utf8PathBuf>,
    reuse_seed: bool,
    json_output: bool,
    providers_path: Option<Utf8PathBuf>,
) -> Result<()> {
    let original_dir = resolve_run_dir(runs_dir, id)?;
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
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
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

async fn export_run_trace(
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
    let run_span_id = qcg_types::span_id_for_scope(id, "run");
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
            "spanId": qcg_types::span_id_for_scope(id, &format!("step:{node}")),
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

fn event_time_unix_nano(event: &qcg_types::RunEvent) -> Result<u128> {
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

fn replay_seed_from_journal(run_dir: &Utf8Path) -> Result<u64> {
    let events = read_run_events(run_dir)?;
    events
        .iter()
        .find_map(|event| match &event.data {
            qcg_types::RunEventData::LlmCall(data) => data.seed,
            _ => None,
        })
        .with_context(|| format!("run `{run_dir}` does not record an LLM seed"))
}

fn gc_runs(runs_dir: &Utf8Path, keep: usize, delete: bool) -> Result<()> {
    gc_runs_impl(runs_dir, keep, delete, true)
}

fn gc_runs_impl(runs_dir: &Utf8Path, keep: usize, delete: bool, report: bool) -> Result<()> {
    if !runs_dir.exists() {
        return Ok(());
    }
    let mut runs: Vec<RunGcCandidate> = Vec::new();
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_CLI_DIRECTORY_SCAN_ENTRIES {
            anyhow::bail!(
                "runs directory contains more than {MAX_CLI_DIRECTORY_SCAN_ENTRIES} entries"
            );
        }
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|path| anyhow::anyhow!("run path is not valid UTF-8: {}", path.display()))?;
        if !path.is_dir() || !run_meta_dir(&path).join("journal.jsonl").exists() {
            continue;
        }
        let summary = run_summary(&path)?;
        if !matches!(summary.status.as_str(), "success" | "failed" | "canceled") {
            continue;
        }
        let retain_days = run_retain_days(&summary)?;
        let expired_by_retain = retain_days
            .map(|days| run_is_older_than(&summary.started_at, days))
            .transpose()?
            .unwrap_or(false);
        runs.push(RunGcCandidate {
            started_at: summary.started_at,
            path,
            expired_by_retain,
        });
    }
    runs.sort_by(|left, right| right.started_at.cmp(&left.started_at));
    for (index, run) in runs.into_iter().enumerate() {
        if index < keep && !run.expired_by_retain {
            continue;
        }
        if delete {
            std::fs::remove_dir_all(&run.path)
                .with_context(|| format!("failed to remove run directory `{}`", run.path))?;
            if report {
                println!("deleted {}", run.path);
            }
        } else if report {
            println!("would_delete {}", run.path);
        }
    }
    Ok(())
}

fn auto_gc_runs(runs_dir: &Utf8Path) -> Result<()> {
    let enabled = std::env::var("QCG_AUTO_GC")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "off"))
        .unwrap_or(true);
    if enabled {
        gc_runs_impl(runs_dir, 50, true, false)?;
    }
    Ok(())
}

struct RunGcCandidate {
    started_at: String,
    path: Utf8PathBuf,
    expired_by_retain: bool,
}

fn run_retain_days(summary: &qcg_service::RunSummary) -> Result<Option<u32>> {
    if summary.retain_days.is_some() {
        return Ok(summary.retain_days);
    }
    let contract =
        Contract::load(Utf8PathBuf::from(&summary.generator_path)).with_context(|| {
            format!(
                "failed to load generator contract `{}` for run `{}` retention",
                summary.generator_path, summary.run_id
            )
        })?;
    Ok(contract.manifest.journal.retain_days)
}

fn run_is_older_than(started_at: &str, retain_days: u32) -> Result<bool> {
    if started_at.trim().is_empty() {
        return Ok(false);
    }
    let started = chrono::DateTime::parse_from_rfc3339(started_at)
        .with_context(|| format!("failed to parse run timestamp `{started_at}`"))?
        .with_timezone(&chrono::Utc);
    let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(retain_days));
    Ok(started < cutoff)
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

fn load_inputs(
    pairs: Vec<String>,
    file: Option<Utf8PathBuf>,
    file_pairs: Vec<String>,
    runtime: &qcg_contract::RuntimeLimits,
) -> Result<BTreeMap<String, Value>> {
    let mut values = if let Some(file) = file {
        let bytes = read_file_bounded(&file, MAX_CLI_JSON_INPUT_BYTES)?;
        serde_json::from_slice::<BTreeMap<String, Value>>(&bytes)?
    } else {
        BTreeMap::new()
    };
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .with_context(|| format!("input `{pair}` must be k=v"))?;
        values.insert(key.to_string(), parse_value(value));
    }
    for pair in file_pairs {
        let (field, path) = pair
            .split_once('=')
            .with_context(|| format!("file input `{pair}` must be field=path"))?;
        if field.is_empty() {
            anyhow::bail!("file input field must not be empty");
        }
        let path = Utf8Path::new(path);
        let name = path
            .file_name()
            .with_context(|| format!("file input path `{path}` has no file name"))?;
        let bytes = read_file_bounded(path, runtime.file_input_limit_bytes)?;
        let value = qcg_types::FileValue::from_bytes_with_limit(
            name,
            &bytes,
            runtime.file_input_limit_bytes,
        )
        .with_context(|| format!("invalid file input `{field}`"))?;
        values.insert(field.to_string(), serde_json::to_value(value)?);
    }
    Ok(values)
}

fn read_file_bounded(path: &Utf8Path, max_bytes: usize) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("failed to read `{path}`"))?;
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read `{path}`"))?;
    if bytes.len() > max_bytes {
        anyhow::bail!("file `{path}` exceeds {max_bytes} bytes");
    }
    Ok(bytes)
}

fn load_answers(pairs: Vec<String>) -> Result<BTreeMap<String, Value>> {
    let mut values = BTreeMap::new();
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .with_context(|| format!("answer `{pair}` must be k=v"))?;
        values.insert(key.to_string(), parse_value(value));
    }
    Ok(values)
}

fn app_registry(providers_path: Option<&Utf8Path>) -> Result<qcg_engine::StepRegistry> {
    let runtime = match qcg_llm::LlmRouter::load_optional(providers_path)? {
        Some(router) => router.into_runtime(),
        // No registry was named and none was found: built-in capabilities stay
        // available while other ids receive setup guidance during validation.
        None => qcg_llm::LlmRuntime::builtins(),
    };
    let mut registry = qcg_steps::deterministic_registry_with_mcp(Arc::new(runtime.mcp.clone()));
    qcg_llm_steps::register_llm_steps(&mut registry, Arc::new(runtime));
    Ok(registry)
}

fn parse_value(value: &str) -> Value {
    if let Ok(json) = serde_json::from_str(value) {
        return json;
    }
    if value == "true" {
        return Value::Bool(true);
    }
    if value == "false" {
        return Value::Bool(false);
    }
    if let Ok(number) = value.parse::<i64>() {
        return Value::Number(number.into());
    }
    Value::String(value.to_string())
}

struct InstallVerification<'a> {
    sha256: Option<&'a str>,
    signature: Option<&'a str>,
    public_key: Option<&'a str>,
}

#[derive(Default)]
struct CleanupPaths {
    paths: Vec<Utf8PathBuf>,
}

impl CleanupPaths {
    fn push(&mut self, path: Utf8PathBuf) {
        self.paths.push(path);
    }
}

impl Drop for CleanupPaths {
    fn drop(&mut self) {
        for path in self.paths.drain(..) {
            let _ = remove_owned_path(&path);
        }
    }
}

struct StagedInstall {
    path: Utf8PathBuf,
    _cleanup: CleanupPaths,
}

struct CleanupPath {
    path: Option<Utf8PathBuf>,
}

impl CleanupPath {
    fn new(path: Utf8PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn cleanup(&mut self) -> Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        remove_owned_path(path)?;
        self.path = None;
        Ok(())
    }

    fn disarm(&mut self) {
        self.path = None;
    }

    fn path(&self) -> Option<&Utf8Path> {
        self.path.as_deref()
    }
}

impl Drop for CleanupPath {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = remove_owned_path(&path);
        }
    }
}

fn remove_owned_path(path: &Utf8Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

async fn install(
    providers_path: Option<&Utf8Path>,
    source: &str,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
    verification: InstallVerification<'_>,
) -> Result<String> {
    let remote = source.starts_with("http://") || source.starts_with("https://");
    if verification.signature.is_some() != verification.public_key.is_some() {
        anyhow::bail!("--signature and --public-key must be supplied together");
    }
    if remote && verification.sha256.is_none() && verification.signature.is_none() {
        anyhow::bail!(
            "remote installs require --sha256 or an Ed25519 --signature with --public-key"
        );
    }
    let staged = stage_install_source(
        source,
        verification.sha256,
        verification.signature.zip(verification.public_key),
    )
    .await?;
    let contract = Contract::load(&staged.path)?;
    app_registry(providers_path)?.validate_contract(&contract)?;
    print_permission_summary(&contract);
    if !yes {
        confirm_stdin("Install this generator?")?;
    }
    let id = contract.manifest.generator.id.clone();
    ensure_safe_install_id(&id)?;
    std::fs::create_dir_all(generators_dir)?;
    let target = generators_dir.join(&id);
    let target_exists = match std::fs::symlink_metadata(&target) {
        Ok(_) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect existing generator `{target}`"));
        }
    };
    if target_exists && !force {
        anyhow::bail!("generator `{id}` already exists at `{target}`; pass --force to replace it");
    }
    if dunce::canonicalize(generators_dir)?.starts_with(dunce::canonicalize(&staged.path)?) {
        anyhow::bail!(
            "install destination `{generators_dir}` is inside source `{}`",
            staged.path
        );
    }
    let temporary = unique_directory_at(generators_dir, "qcg-install-temp")?;
    let mut temporary = CleanupPath::new(temporary);
    copy_dir_all(
        &staged.path,
        temporary.path().expect("temporary path must be armed"),
    )?;
    let copied_contract = Contract::load(temporary.path().expect("temporary path must be armed"))?;
    if copied_contract.manifest.generator.id != id {
        anyhow::bail!("copied generator manifest id does not match `{id}`");
    }
    app_registry(providers_path)?.validate_contract(&copied_contract)?;
    commit_install(
        temporary.path().expect("temporary path must be armed"),
        &target,
        target_exists,
    )?;
    temporary.disarm();
    Ok(id)
}

fn commit_install(temporary: &Utf8Path, target: &Utf8Path, replace_existing: bool) -> Result<()> {
    let parent = target
        .parent()
        .context("install target must have a parent directory")?;
    let backup = if replace_existing {
        let backup = loop {
            let candidate = unique_nonexistent_path(parent, "qcg-install-backup")?;
            match std::fs::rename(target, &candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to move existing generator `{target}`"));
                }
            }
        };
        Some(CleanupPath::new(backup))
    } else {
        None
    };

    if !replace_existing {
        match std::fs::symlink_metadata(target) {
            Ok(_) => anyhow::bail!("install target `{target}` appeared during staging"),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install target `{target}`"));
            }
        }
    }

    if let Err(error) = std::fs::rename(temporary, target) {
        let Some(mut backup) = backup else {
            return Err(error).with_context(|| format!("failed to commit generator `{target}`"));
        };
        let backup_path = backup
            .path()
            .map(|path| path.to_owned())
            .expect("backup path must be armed");
        match std::fs::rename(&backup_path, target) {
            Ok(()) => {
                backup.disarm();
                return Err(error)
                    .with_context(|| format!("failed to commit generator `{target}`"));
            }
            Err(restore_error) => {
                // Keep the backup when restoration itself fails so the existing install remains recoverable.
                backup.disarm();
                return Err(anyhow::anyhow!(
                    "failed to commit generator `{target}`: {error}; failed to restore existing generator: {restore_error}; backup remains at `{backup_path}`"
                ));
            }
        }
    }

    if let Some(mut backup) = backup {
        backup
            .cleanup()
            .with_context(|| format!("installed `{target}` but failed to remove backup"))?;
    }
    Ok(())
}

fn uninstall(id: &str, generators_dir: &Utf8Path, yes: bool) -> Result<()> {
    ensure_safe_install_id(id)?;
    let target = generators_dir.join(id);
    if !target.join("qcg.toml").exists() {
        anyhow::bail!("generator `{id}` is not installed under `{generators_dir}`");
    }
    if !yes {
        confirm_stdin(&format!("Uninstall generator `{id}`?"))?;
    }
    std::fs::remove_dir_all(&target)
        .with_context(|| format!("failed to remove generator `{target}`"))?;
    Ok(())
}

async fn stage_install_source(
    source: &str,
    expected_sha256: Option<&str>,
    signature: Option<(&str, &str)>,
) -> Result<StagedInstall> {
    let mut cleanup = CleanupPaths::default();
    let source_path = if source.starts_with("http://") || source.starts_with("https://") {
        let response = reqwest::get(source).await?.error_for_status()?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_PACKAGE_ARCHIVE_BYTES as u64)
        {
            anyhow::bail!("remote package exceeds 256 MiB");
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let next = bytes
                .len()
                .checked_add(chunk.len())
                .context("remote package size overflowed")?;
            if next > MAX_PACKAGE_ARCHIVE_BYTES {
                anyhow::bail!("remote package exceeds 256 MiB");
            }
            bytes.extend_from_slice(&chunk);
        }
        verify_package_bytes(&bytes, expected_sha256, signature)?;
        let temp_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir()).map_err(|path| {
            anyhow::anyhow!(
                "temporary directory path is not valid UTF-8: {}",
                path.display()
            )
        })?;
        let archive = unique_file_at(&temp_dir, "qcg-install-archive", "qcg")?;
        cleanup.push(archive.clone());
        std::fs::write(&archive, bytes)?;
        archive
    } else {
        Utf8PathBuf::from(source)
    };
    if source_path.is_dir() {
        if expected_sha256.is_some() || signature.is_some() {
            anyhow::bail!("checksum and signature verification require a package archive");
        }
        return Ok(StagedInstall {
            path: source_path,
            _cleanup: cleanup,
        });
    }
    if !source.starts_with("http://") && !source.starts_with("https://") {
        let bytes = read_file_bounded(&source_path, MAX_PACKAGE_ARCHIVE_BYTES)?;
        verify_package_bytes(&bytes, expected_sha256, signature)?;
    }
    let stage = unique_stage_dir()?;
    cleanup.push(stage.clone());
    unpack_qcg(&source_path, &stage)?;
    Ok(StagedInstall {
        path: stage,
        _cleanup: cleanup,
    })
}

fn verify_package_bytes(
    bytes: &[u8],
    expected_sha256: Option<&str>,
    signature: Option<(&str, &str)>,
) -> Result<()> {
    if let Some(expected) = expected_sha256 {
        let expected = expected.trim().to_ascii_lowercase();
        if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            anyhow::bail!("--sha256 must be a 64-character hexadecimal digest");
        }
        let actual = hex::encode(Sha256::digest(bytes));
        if actual != expected {
            anyhow::bail!("package SHA-256 mismatch: expected {expected}, got {actual}");
        }
    }
    if let Some((signature, public_key)) = signature {
        let signature = hex::decode(signature.trim())
            .context("--signature must be hexadecimal Ed25519 signature bytes")?;
        let public_key = hex::decode(public_key.trim())
            .context("--public-key must be hexadecimal Ed25519 public-key bytes")?;
        UnparsedPublicKey::new(&ED25519, public_key)
            .verify(bytes, &signature)
            .map_err(|_| anyhow::anyhow!("package Ed25519 signature verification failed"))?;
    }
    Ok(())
}

fn sign_package(output: &Utf8Path, bytes: &[u8], signing_key: &Utf8Path) -> Result<()> {
    let pkcs8 = read_file_bounded(signing_key, MAX_SIGNING_KEY_BYTES)
        .with_context(|| format!("failed to read Ed25519 PKCS#8 key `{signing_key}`"))?;
    let key = Ed25519KeyPair::from_pkcs8(&pkcs8)
        .map_err(|_| anyhow::anyhow!("`{signing_key}` is not a valid Ed25519 PKCS#8 key"))?;
    let signature_path = Utf8PathBuf::from(format!("{output}.sig"));
    let public_key_path = Utf8PathBuf::from(format!("{output}.pub"));
    std::fs::write(&signature_path, hex::encode(key.sign(bytes).as_ref()))?;
    std::fs::write(&public_key_path, hex::encode(key.public_key().as_ref()))?;
    println!("signature {signature_path}");
    println!("public_key {public_key_path}");
    Ok(())
}

fn unique_stage_dir() -> Result<Utf8PathBuf> {
    let parent = Utf8PathBuf::from_path_buf(std::env::temp_dir()).map_err(|path| {
        anyhow::anyhow!(
            "temporary directory path is not valid UTF-8: {}",
            path.display()
        )
    })?;
    unique_directory_at(&parent, "qcg-install-stage")
}

fn unique_directory_at(parent: &Utf8Path, prefix: &str) -> Result<Utf8PathBuf> {
    loop {
        let path = parent.join(format!(".{prefix}-{}", Uuid::now_v7()));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create temporary directory `{path}`"));
            }
        }
    }
}

fn unique_file_at(parent: &Utf8Path, prefix: &str, extension: &str) -> Result<Utf8PathBuf> {
    loop {
        let path = parent.join(format!(".{prefix}-{}.{}", Uuid::now_v7(), extension));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => return Ok(path),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create temporary file `{path}`"));
            }
        }
    }
}

fn unique_nonexistent_path(parent: &Utf8Path, prefix: &str) -> Result<Utf8PathBuf> {
    loop {
        let path = parent.join(format!(".{prefix}-{}", Uuid::now_v7()));
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(path),
            Ok(_) => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect temporary path `{path}`"));
            }
        }
    }
}

fn unpack_qcg(archive: &Utf8Path, target: &Utf8Path) -> Result<()> {
    let file = File::open(archive).with_context(|| format!("failed to open `{archive}`"))?;
    let mut archive = zip::ZipArchive::new(file)?;
    if archive.len() > MAX_PACKAGE_ENTRIES {
        anyhow::bail!("archive contains too many entries");
    }
    let mut unpacked_bytes = 0_u64;
    let mut paths = BTreeSet::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let Some(enclosed) = entry.enclosed_name() else {
            anyhow::bail!("archive contains an unsafe path: {}", entry.name());
        };
        let rel = Utf8PathBuf::from_path_buf(enclosed.to_path_buf())
            .map_err(|_| anyhow::anyhow!("archive path is not UTF-8"))?;
        if rel.as_str().is_empty() {
            continue;
        }
        if !paths.insert(rel.clone()) {
            anyhow::bail!("archive contains duplicate path `{rel}`");
        }
        if entry
            .unix_mode()
            .is_some_and(|mode| mode & 0o170000 == 0o120000)
        {
            anyhow::bail!("archive contains a symbolic link `{rel}`");
        }
        unpacked_bytes = unpacked_bytes
            .checked_add(entry.size())
            .context("archive expanded size overflowed")?;
        if unpacked_bytes > MAX_PACKAGE_BYTES {
            anyhow::bail!("archive expanded size exceeds 256 MiB");
        }
        let out = target.join(&rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&out)?;
        } else {
            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut output = File::create(&out)?;
            let copied = std::io::copy(&mut entry, &mut output)?;
            if copied != entry.size() {
                anyhow::bail!("archive entry size changed while unpacking `{rel}`");
            }
        }
    }
    verify_package_inventory(target)?;
    Ok(())
}

fn copy_dir_all(source: &Utf8Path, target: &Utf8Path) -> Result<()> {
    let mut entry_count = 0_usize;
    let mut copied_bytes = 0_u64;
    for entry in WalkDir::new(source) {
        let entry = entry.with_context(|| format!("failed to walk package source `{source}`"))?;
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            anyhow::bail!(
                "package source contains a symbolic link: {}",
                entry.path().display()
            );
        }
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            anyhow::anyhow!("source path is not valid UTF-8: {}", path.display())
        })?;
        let rel = path.strip_prefix(source)?;
        if rel.as_str().is_empty() {
            if !file_type.is_dir() {
                anyhow::bail!("package source is not a directory: {path}");
            }
            continue;
        }
        entry_count = entry_count
            .checked_add(1)
            .context("package entry count overflowed")?;
        if entry_count > MAX_PACKAGE_ENTRIES {
            anyhow::bail!("package source contains too many entries");
        }
        if !qcg_types::is_safe_relative_path(&portable_relative_path(rel)) {
            anyhow::bail!("package source contains an unsafe path `{rel}`");
        }
        let dest = target.join(rel);
        if file_type.is_dir() {
            std::fs::create_dir_all(&dest)?;
            continue;
        }
        if !file_type.is_file() {
            anyhow::bail!("package source contains an unsupported entry: {path}");
        }
        let metadata = entry.metadata()?;
        copied_bytes = copied_bytes
            .checked_add(metadata.len())
            .context("package source size overflowed")?;
        if copied_bytes > MAX_PACKAGE_BYTES {
            anyhow::bail!("package source exceeds 256 MiB");
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let copied = std::fs::copy(&path, &dest)?;
        if copied != metadata.len() {
            anyhow::bail!("package source changed while being copied: {path}");
        }
        if copied > MAX_PACKAGE_BYTES.saturating_sub(copied_bytes - metadata.len()) {
            anyhow::bail!("package source exceeds 256 MiB");
        }
    }
    Ok(())
}

fn print_permission_summary(contract: &Contract) {
    let permissions = &contract.manifest.permissions;
    println!(
        "generator: {}@{}",
        contract.manifest.generator.id, contract.manifest.generator.version
    );
    println!("permissions:");
    println!("  fs_read: {}", join_or_none(&permissions.fs_read));
    println!("  fs_write: {}", join_or_none(&permissions.fs_write));
    println!("  network: {}", join_or_none(&permissions.network));
    println!(
        "  commands: {}",
        if permissions.commands.is_empty() {
            "none".into()
        } else {
            permissions
                .commands
                .iter()
                .map(|command| {
                    format!(
                        "{} {:?} isolation={:?} image={}",
                        command.bin,
                        command.args,
                        command.isolation,
                        command.image.as_deref().unwrap_or("none")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    println!(
        "  containers: enabled={}, runtime={}, images={}, on_missing={}",
        permissions.containers.enabled,
        permissions
            .containers
            .runtime
            .map(|runtime| format!("{runtime:?}"))
            .unwrap_or_else(|| "none".into()),
        join_or_none(&permissions.containers.images),
        permissions
            .containers
            .on_missing
            .as_deref()
            .unwrap_or("error")
    );
    println!("  side_effects: {:?}", permissions.side_effects);
    println!(
        "  secrets: {}",
        if contract.manifest.secrets.is_empty() {
            "none".into()
        } else {
            contract
                .manifest
                .secrets
                .iter()
                .map(|(name, secret)| {
                    format!(
                        "{name} ({})",
                        secret.source_label().unwrap_or_else(|| "invalid".into())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
}

fn join_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}

fn confirm_stdin(prompt: &str) -> Result<()> {
    eprint!("{prompt} [y/N] ");
    std::io::stderr().flush()?;
    let answer = read_bounded_confirmation(&mut std::io::stdin().lock())?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        anyhow::bail!("operation cancelled")
    }
}

fn read_bounded_confirmation<R: std::io::BufRead>(reader: &mut R) -> Result<String> {
    let mut bytes = Vec::with_capacity(MAX_CONFIRM_INPUT_BYTES);
    reader
        .take((MAX_CONFIRM_INPUT_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.len() > MAX_CONFIRM_INPUT_BYTES {
        anyhow::bail!("confirmation input exceeds {MAX_CONFIRM_INPUT_BYTES} bytes");
    }
    String::from_utf8(bytes).context("confirmation input must be valid UTF-8")
}

fn ensure_safe_install_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.starts_with('/')
        || id.contains('\\')
        || id.contains('\0')
        || id.split('/').any(|part| part == ".." || part.is_empty())
    {
        anyhow::bail!("generator id `{id}` is not safe for installation");
    }
    Ok(())
}

struct PackageFileEntry {
    archive_path: String,
    source_path: Utf8PathBuf,
    bytes: u64,
    sha256: String,
    metadata: std::fs::Metadata,
}

fn package(dir: &Utf8Path, output: &Utf8Path) -> Result<()> {
    const SBOM_PATH: &str = "QCG-SBOM.spdx.json";
    const PROVENANCE_PATH: &str = "QCG-PROVENANCE.intoto.json";
    let root = dunce::canonicalize(dir)
        .with_context(|| format!("failed to canonicalize package input `{dir}`"))?;
    let output_absolute = if output.is_absolute() {
        output.to_path_buf().into_std_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let output_parent = output_absolute
        .parent()
        .context("package output must have a parent directory")?;
    let output_parent = dunce::canonicalize(output_parent).with_context(|| {
        format!(
            "failed to canonicalize package output directory `{}`",
            output_parent.display()
        )
    })?;
    if output_parent.starts_with(&root) {
        anyhow::bail!("package output must be outside the package input directory");
    }
    let contract = Contract::load(dir)?;
    let mut directories = Vec::<(String, std::fs::Metadata)>::new();
    let mut entries = Vec::<PackageFileEntry>::new();
    let mut total_bytes = 0_u64;
    for entry in WalkDir::new(dir) {
        let entry = entry?;
        if entry.file_type().is_symlink() {
            anyhow::bail!(
                "package input contains a symbolic link: {}",
                entry.path().display()
            );
        }
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            anyhow::anyhow!("package path is not valid UTF-8: {}", path.display())
        })?;
        let relative = path.strip_prefix(dir)?;
        if relative.as_str().is_empty() {
            continue;
        }
        let name = portable_relative_path(relative);
        if matches!(name.as_str(), SBOM_PATH | PROVENANCE_PATH) {
            anyhow::bail!("package input uses reserved metadata path `{name}`");
        }
        if directories.len() + entries.len() >= MAX_PACKAGE_SOURCE_ENTRIES {
            anyhow::bail!("package input contains too many entries");
        }
        let metadata = entry.metadata()?;
        if entry.file_type().is_dir() {
            directories.push((name, metadata));
            continue;
        }
        if !entry.file_type().is_file() {
            anyhow::bail!("package input contains an unsupported entry: {path}");
        }
        let bytes = metadata.len();
        total_bytes = total_bytes
            .checked_add(bytes)
            .context("package input size overflowed")?;
        if total_bytes > MAX_PACKAGE_BYTES {
            anyhow::bail!("package input exceeds 256 MiB");
        }
        let sha256 = sha256_file(&path)?;
        entries.push(PackageFileEntry {
            archive_path: name,
            source_path: path,
            bytes,
            sha256,
            metadata,
        });
    }
    directories.sort_by(|left, right| left.0.cmp(&right.0));
    entries.sort_by(|left, right| left.archive_path.cmp(&right.archive_path));
    let files = entries
        .iter()
        .map(|entry| {
            json!({
                "fileName": entry.archive_path,
                "checksums": [{"algorithm": "SHA256", "checksumValue": entry.sha256}],
                "size": entry.bytes,
            })
        })
        .collect::<Vec<_>>();
    let sbom = serde_json::to_vec_pretty(&json!({
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": format!("{}-{}", contract.manifest.generator.id, contract.manifest.generator.version),
        "documentNamespace": format!("https://qcg.local/spdx/{}/{}", contract.manifest.generator.id, contract.sha256),
        "files": files,
    }))?;
    let materials = entries
        .iter()
        .map(|entry| json!({"uri": entry.archive_path, "digest": {"sha256": entry.sha256}}))
        .collect::<Vec<_>>();
    let provenance = serde_json::to_vec_pretty(&json!({
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [{
            "name": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
            "digest": {"sha256": contract.sha256}
        }],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": "https://qcg.local/package/v1",
                "externalParameters": {},
                "internalParameters": {},
                "resolvedDependencies": materials
            },
            "runDetails": {
                "builder": {"id": format!("qcg/{}", env!("CARGO_PKG_VERSION"))},
                "metadata": {"invocationId": contract.sha256}
            }
        }
    }))?;
    let file = File::create(output)?;
    let mut zip = zip::ZipWriter::new(file);
    for (name, metadata) in &directories {
        zip.add_directory(format!("{name}/"), package_zip_options(metadata, true)?)?;
    }
    for entry in &entries {
        zip.start_file(
            &entry.archive_path,
            package_zip_options(&entry.metadata, false)?,
        )?;
        let (bytes, sha256) = copy_file_with_sha256(&entry.source_path, &mut zip)?;
        if bytes != entry.bytes || sha256 != entry.sha256 {
            anyhow::bail!(
                "package input changed while being archived: {}",
                entry.source_path
            );
        }
    }
    let generated_options = generated_package_zip_options(SystemTime::now())?;
    for (name, bytes) in [(SBOM_PATH, sbom), (PROVENANCE_PATH, provenance)] {
        zip.start_file(name, generated_options)?;
        zip.write_all(&bytes)?;
    }
    zip.finish()?;
    Ok(())
}

fn package_zip_options(metadata: &std::fs::Metadata, directory: bool) -> Result<SimpleFileOptions> {
    let mut options = SimpleFileOptions::default().compression_method(if directory {
        zip::CompressionMethod::Stored
    } else {
        zip::CompressionMethod::Deflated
    });
    options = with_zip_modified_time(options, metadata.modified()?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        options = options.unix_permissions(metadata.permissions().mode());
    }
    #[cfg(not(unix))]
    {
        let writable = !metadata.permissions().readonly();
        let permissions = match (directory, writable) {
            (true, true) => 0o755,
            (true, false) => 0o555,
            (false, true) => 0o644,
            (false, false) => 0o444,
        };
        options = options.unix_permissions(permissions);
    }
    Ok(options)
}

fn generated_package_zip_options(modified: SystemTime) -> Result<SimpleFileOptions> {
    with_zip_modified_time(
        SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o644),
        modified,
    )
}

fn with_zip_modified_time(
    mut options: SimpleFileOptions,
    modified: SystemTime,
) -> Result<SimpleFileOptions> {
    let modified = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
    let modified = zip::DateTime::try_from(modified).map_err(|error| {
        anyhow::anyhow!("modification time is not representable in ZIP: {error}")
    })?;
    options = options.last_modified_time(modified);
    Ok(options)
}

fn portable_relative_path(path: &Utf8Path) -> String {
    path.components()
        .map(|component| component.as_str())
        .collect::<Vec<_>>()
        .join("/")
}

fn sha256_file(path: &Utf8Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn copy_file_with_sha256<W: Write>(path: &Utf8Path, writer: &mut W) -> Result<(u64, String)> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("package input size overflowed while archiving")?;
    }
    Ok((bytes, hex::encode(digest.finalize())))
}

fn verify_package_inventory(root: &Utf8Path) -> Result<()> {
    let sbom_path = root.join("QCG-SBOM.spdx.json");
    let provenance_path = root.join("QCG-PROVENANCE.intoto.json");
    let sbom: Value = serde_json::from_slice(
        &read_file_bounded(&sbom_path, MAX_PACKAGE_METADATA_BYTES)
            .context("package is missing QCG-SBOM.spdx.json")?,
    )?;
    let provenance: Value = serde_json::from_slice(
        &read_file_bounded(&provenance_path, MAX_PACKAGE_METADATA_BYTES)
            .context("package is missing QCG-PROVENANCE.intoto.json")?,
    )?;
    if sbom.get("spdxVersion").and_then(Value::as_str) != Some("SPDX-2.3")
        || provenance.get("_type").and_then(Value::as_str)
            != Some("https://in-toto.io/Statement/v1")
    {
        anyhow::bail!("package supply-chain metadata has an unsupported format");
    }
    let mut expected = BTreeMap::new();
    for file in sbom
        .get("files")
        .and_then(Value::as_array)
        .context("package SBOM files array is required")?
    {
        let path = file
            .get("fileName")
            .and_then(Value::as_str)
            .context("package SBOM fileName is required")?;
        if !qcg_types::is_safe_relative_path(path) {
            anyhow::bail!("package SBOM contains unsafe path `{path}`");
        }
        let sha256 = file
            .pointer("/checksums/0/checksumValue")
            .and_then(Value::as_str)
            .context("package SBOM SHA256 checksum is required")?;
        if expected
            .insert(path.to_string(), sha256.to_string())
            .is_some()
        {
            anyhow::bail!("package SBOM contains duplicate path `{path}`");
        }
    }
    let mut actual = BTreeSet::new();
    for entry in WalkDir::new(root) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            anyhow::anyhow!("package inventory path is not UTF-8: {}", path.display())
        })?;
        let relative = portable_relative_path(path.strip_prefix(root)?);
        if matches!(
            relative.as_str(),
            "QCG-SBOM.spdx.json" | "QCG-PROVENANCE.intoto.json"
        ) {
            continue;
        }
        actual.insert(relative.clone());
        let expected_sha256 = expected
            .get(&relative)
            .with_context(|| format!("package contains unlisted file `{relative}`"))?;
        let digest = sha256_file(&path)?;
        if &digest != expected_sha256 {
            anyhow::bail!("package file `{relative}` failed SBOM integrity verification");
        }
    }
    if actual != expected.keys().cloned().collect() {
        anyhow::bail!("package SBOM lists files that are missing from the archive");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;

    #[test]
    fn confirmation_reader_is_bounded() {
        let mut exact = std::io::Cursor::new(format!("{}\n", "x".repeat(MAX_CONFIRM_INPUT_BYTES)));
        assert_eq!(
            read_bounded_confirmation(&mut exact)
                .expect("confirmation at the exact limit should pass")
                .len(),
            MAX_CONFIRM_INPUT_BYTES
        );

        let mut excessive =
            std::io::Cursor::new(format!("{}\n", "x".repeat(MAX_CONFIRM_INPUT_BYTES + 1)));
        assert!(
            read_bounded_confirmation(&mut excessive)
                .expect_err("confirmation above the limit must fail")
                .to_string()
                .contains("exceeds")
        );
    }

    #[test]
    fn silent_gc_honors_retain_days_without_reporting() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-gc-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let runs_dir = root.join("runs");
        let run_dir = runs_dir.join("old-run");
        let generator_dir = root.join("generator");
        std::fs::create_dir_all(run_meta_dir(&run_dir))
            .expect("run metadata directory should be created");
        std::fs::create_dir_all(&generator_dir).expect("generator directory should be created");
        std::fs::write(
            generator_dir.join("qcg.toml"),
            r#"
[generator]
id = "gc-fixture"
name = "GC Fixture"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "noop"
type = "write"

[flow.params]
output_file = "noop.txt"
content = "noop"

[journal]
retain_days = 0

[outputs]
extras = []
"#,
        )
        .expect("generator manifest should be written");
        let journal = qcg_engine::JournalWriter::create(
            &run_meta_dir(&run_dir).join("journal.jsonl"),
            "old-run",
            false,
            None,
        )
        .expect("journal should be created");
        journal
            .event(
                "run_started",
                json!({
                    "generator": "gc-fixture",
                    "generator_path": generator_dir,
                    "contract_sha256": "fixture",
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                    "retain_days": 0,
                }),
            )
            .expect("run should start");
        journal
            .event("run_finished", json!({ "status": "success" }))
            .expect("run should finish");

        gc_runs_impl(&runs_dir, 50, true, false).expect("silent GC should run");

        assert!(!run_dir.exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn package_verification_accepts_pinned_hash_and_ed25519_signature() {
        let bytes = b"bounded generator package";
        let digest = hex::encode(Sha256::digest(bytes));
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let signature = hex::encode(key.sign(bytes).as_ref());
        let public_key = hex::encode(key.public_key().as_ref());
        verify_package_bytes(bytes, Some(&digest), Some((&signature, &public_key))).unwrap();
        assert!(
            verify_package_bytes(b"tampered", Some(&digest), Some((&signature, &public_key)))
                .is_err()
        );
    }

    #[test]
    fn copy_dir_all_propagates_walk_errors_and_rejects_symlinks() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-copy-test-{}", Uuid::now_v7()));
        let source = root.join("source");
        let target = root.join("target");
        std::fs::create_dir_all(&source).expect("source directory should be created");
        std::fs::write(source.join("file.txt"), "copy me").expect("source file should be written");
        assert!(copy_dir_all(&root.join("missing"), &target).is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(source.join("file.txt"), source.join("link.txt"))
                .expect("symbolic link should be created");
            assert!(copy_dir_all(&source, &target).is_err());
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn commit_install_replaces_existing_tree_and_cleans_backup() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-commit-test-{}", Uuid::now_v7()));
        let target = root.join("generator");
        let temporary = root.join("staged");
        std::fs::create_dir_all(&target).expect("target directory should be created");
        std::fs::create_dir_all(&temporary).expect("temporary directory should be created");
        std::fs::write(target.join("old.txt"), "old").expect("old file should be written");
        std::fs::write(temporary.join("new.txt"), "new").expect("new file should be written");

        commit_install(&temporary, &target, true).expect("install commit should succeed");

        assert!(!target.join("old.txt").exists());
        assert_eq!(
            std::fs::read_to_string(target.join("new.txt")).unwrap(),
            "new"
        );
        assert!(!temporary.exists());
        let leftovers = std::fs::read_dir(&root)
            .expect("commit directory should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                name.starts_with(".qcg-install-backup-") || name.starts_with(".qcg-install-temp-")
            })
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "transaction leftovers: {leftovers:?}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn failed_install_commit_cleanup_does_not_remove_user_source() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-commit-failure-test-{}", Uuid::now_v7()));
        let source = root.join("source");
        let temporary = root.join("temporary");
        let target = root.join("missing-parent").join("generator");
        std::fs::create_dir_all(&source).expect("source directory should be created");
        std::fs::create_dir_all(&temporary).expect("temporary directory should be created");
        std::fs::write(source.join("source.txt"), "keep").expect("source file should be written");
        std::fs::write(temporary.join("new.txt"), "new").expect("new file should be written");
        let temporary_guard = CleanupPath::new(temporary.clone());

        assert!(commit_install(&temporary, &target, false).is_err());
        drop(temporary_guard);
        assert!(source.join("source.txt").exists());
        assert!(!temporary.exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn package_preserves_portable_directories_timestamps_and_permissions() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-package-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let source = root.join("source");
        std::fs::create_dir_all(source.join("nested/empty"))
            .expect("empty directory should be created");
        std::fs::write(
            source.join("qcg.toml"),
            r#"
[generator]
id = "package-fixture"
name = "Package Fixture"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "emit"
type = "write"

[flow.params]
output_file = "result.txt"
content = "result"
"#,
        )
        .expect("manifest should be written");
        let source_file = source.join("nested/file.txt");
        std::fs::write(&source_file, "package content").expect("source file should be written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&source_file, std::fs::Permissions::from_mode(0o640))
                .expect("source permissions should be set");
        }
        let output = root.join("generator.qcg");

        package(&source, &output).expect("package should be written");

        let file = File::open(&output).expect("package should open");
        let mut archive = zip::ZipArchive::new(file).expect("package should parse");
        assert!(
            archive
                .by_name("nested/")
                .expect("nested directory entry")
                .is_dir()
        );
        assert!(
            archive
                .by_name("nested/empty/")
                .expect("empty directory entry")
                .is_dir()
        );
        let entry = archive
            .by_name("nested/file.txt")
            .expect("portable file entry");
        assert!(
            entry.last_modified().expect("file timestamp").year() > 1980,
            "source timestamp must be retained"
        );
        #[cfg(unix)]
        assert_eq!(entry.unix_mode().expect("file permissions") & 0o777, 0o640);
        #[cfg(not(unix))]
        assert_eq!(entry.unix_mode().expect("file permissions") & 0o777, 0o644);
        drop(entry);
        let generated = archive
            .by_name("QCG-SBOM.spdx.json")
            .expect("generated metadata entry");
        assert!(
            generated
                .last_modified()
                .expect("generated metadata timestamp")
                .year()
                > 1980,
            "generated metadata must use the package creation time"
        );
        drop(generated);
        drop(archive);
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn replay_seed_is_read_from_original_llm_call() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-replay-seed-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(run_meta_dir(&root))
            .expect("run metadata directory should be created");
        let journal = qcg_engine::JournalWriter::create(
            &run_meta_dir(&root).join("journal.jsonl"),
            "seed-run",
            false,
            None,
        )
        .expect("journal should be created");
        journal
            .event(
                "llm_call",
                json!({
                    "node": "generate",
                    "provider": "fake",
                    "model": "fake",
                    "seed": 12345_u64,
                    "max_tokens": 128,
                    "tokens": { "input": 0, "output": 0 },
                    "cost_microusd": 0,
                }),
            )
            .expect("LLM call should be recorded");

        let seed = replay_seed_from_journal(&root).expect("seed should be read");

        assert_eq!(seed, 12345);
        let _ = std::fs::remove_dir_all(&root);
    }
}
