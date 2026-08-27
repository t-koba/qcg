use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand, ValueEnum};
use qcg_contract::Contract;
use qcg_engine::{OutputManifest, read_output_manifest};
use qcg_service::{
    DirectRun, LocalQcgService, list_run_summaries, read_journal_events, read_run_generator_path,
    read_run_inputs, resolve_run_dir, run_meta_dir, run_summary, step_param_schemas_markdown,
};
use qcg_steps::deterministic_registry;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

#[derive(Debug, Parser)]
#[command(name = "qcg", version, about = "quick config generator")]
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
        help = "Path to the LLM providers registry; defaults to ./providers.toml"
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
            long = "cors-origin",
            env = "QCG_CORS_ORIGIN",
            value_delimiter = ',',
            value_name = "ORIGIN"
        )]
        cors_origins: Vec<String>,
    },
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
            let inputs = load_inputs(inputs, inputs_file, input_files)?;
            let answers = load_answers(answers)?;
            let runs_dir = Utf8PathBuf::from(".qcg/runs");
            auto_gc_runs(&runs_dir)?;
            let service = LocalQcgService::new(Utf8PathBuf::new(), runs_dir, providers_path)?;
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
        Command::List { generators_dir } => {
            let mut roots = vec![generators_dir.clone()];
            if let Some(bundled) = bundled_generators_root()
                && bundled != generators_dir
            {
                roots.push(bundled);
            }
            let mut seen = BTreeSet::new();
            for root in &roots {
                if !root.exists() {
                    continue;
                }
                for entry in std::fs::read_dir(root)? {
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
            RunsCommand::Gc {
                runs_dir,
                keep,
                delete,
            } => gc_runs(&runs_dir, keep, delete)?,
        },
        Command::Package { dir, output } => {
            let output = output.unwrap_or_else(|| {
                Utf8PathBuf::from(format!("{}.qcg", dir.file_name().unwrap_or("generator")))
            });
            package(&dir, &output)?;
            println!("packaged {output}");
        }
        Command::Install {
            source,
            generators_dir,
            yes,
            force,
        } => {
            let installed = install(
                providers_path.as_deref(),
                &source,
                &generators_dir,
                yes,
                force,
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
            cors_origins,
        } => {
            auto_gc_runs(&runs_dir)?;
            if bind
                .parse::<std::net::IpAddr>()
                .map(|address| !address.is_loopback())
                .unwrap_or(true)
            {
                tracing::warn!("unauthenticated API — deploy behind qpx with qid");
            }
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
                    cors_origins,
                },
                listener,
            )
            .await?;
        }
    }
    Ok(())
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
    if !summary.finished_at.is_empty() {
        println!("finished_at: {}", summary.finished_at);
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
    if let Some(artifacts) = summary.artifacts.as_array() {
        println!("artifacts:");
        for artifact in artifacts {
            let path = artifact.get("path").and_then(Value::as_str).unwrap_or("");
            let sha256 = artifact.get("sha256").and_then(Value::as_str).unwrap_or("");
            let bytes = artifact.get("bytes").and_then(Value::as_u64).unwrap_or(0);
            println!("  {path}\t{bytes} bytes\t{sha256}");
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
        None => read_run_generator_path(&original_dir)?.with_context(|| {
            format!("run `{id}` does not record generator_path; pass a generator path explicitly")
        })?,
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

fn replay_seed_from_journal(run_dir: &Utf8Path) -> Result<u64> {
    let events = read_journal_events(run_dir)?;
    events
        .iter()
        .find(|event| event.get("t").and_then(Value::as_str) == Some("llm_call"))
        .and_then(|event| event.get("seed"))
        .and_then(Value::as_u64)
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
    for entry in std::fs::read_dir(runs_dir)? {
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
    let Some(generator_path) = summary.generator_path.as_str() else {
        return Ok(None);
    };
    let contract = Contract::load(Utf8PathBuf::from(generator_path)).with_context(|| {
        format!(
            "failed to load generator contract `{generator_path}` for run `{}` retention",
            summary.run_id
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
) -> Result<BTreeMap<String, Value>> {
    let mut values = if let Some(file) = file {
        let text =
            std::fs::read_to_string(&file).with_context(|| format!("failed to read `{file}`"))?;
        serde_json::from_str::<BTreeMap<String, Value>>(&text)?
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
        let bytes =
            std::fs::read(path).with_context(|| format!("failed to read file input `{path}`"))?;
        let value = qcg_types::FileValue::from_bytes(name, &bytes)
            .with_context(|| format!("invalid file input `{field}`"))?;
        values.insert(field.to_string(), serde_json::to_value(value)?);
    }
    Ok(values)
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
    let mut registry = deterministic_registry();
    let runtime = match qcg_llm::LlmRouter::load_optional(providers_path)? {
        Some(router) => router.into_runtime(),
        // No registry was named and none was found: the built-in `fake`
        // provider keeps fake-only generators working; other ids receive
        // registry-setup guidance during validation.
        None => qcg_llm::LlmRuntime::fake_only(),
    };
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

async fn install(
    providers_path: Option<&Utf8Path>,
    source: &str,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
) -> Result<String> {
    if source.starts_with("http://") || source.starts_with("https://") {
        eprintln!(
            "warning: URL installs are not authenticated and do not verify a checksum or signature; verify the source before continuing"
        );
    }
    let staged = stage_install_source(source).await?;
    let contract = Contract::load(&staged)?;
    app_registry(providers_path)?.validate_contract(&contract)?;
    print_permission_summary(&contract);
    if !yes {
        confirm_stdin("Install this generator?")?;
    }
    let id = contract.manifest.generator.id.clone();
    ensure_safe_install_id(&id)?;
    std::fs::create_dir_all(generators_dir)?;
    let target = generators_dir.join(&id);
    if target.exists() {
        if !force {
            anyhow::bail!(
                "generator `{id}` already exists at `{target}`; pass --force to replace it"
            );
        }
        std::fs::remove_dir_all(&target)
            .with_context(|| format!("failed to replace existing generator `{target}`"))?;
    }
    copy_dir_all(&staged, &target)?;
    Ok(id)
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

async fn stage_install_source(source: &str) -> Result<Utf8PathBuf> {
    let source_path = if source.starts_with("http://") || source.starts_with("https://") {
        let response = reqwest::get(source).await?.error_for_status()?;
        let bytes = response.bytes().await?;
        let archive = unique_stage_dir()?.with_extension("qcg");
        std::fs::write(&archive, bytes)?;
        archive
    } else {
        Utf8PathBuf::from(source)
    };
    if source_path.is_dir() {
        return Ok(source_path);
    }
    let stage = unique_stage_dir()?;
    std::fs::create_dir_all(&stage)?;
    unpack_qcg(&source_path, &stage)?;
    Ok(stage)
}

fn unique_stage_dir() -> Result<Utf8PathBuf> {
    let dir = std::env::temp_dir().join(format!(
        "qcg-install-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after unix epoch")
            .as_millis()
    ));
    Utf8PathBuf::from_path_buf(dir).map_err(|_| anyhow::anyhow!("temporary path is not UTF-8"))
}

fn unpack_qcg(archive: &Utf8Path, target: &Utf8Path) -> Result<()> {
    let file = File::open(archive).with_context(|| format!("failed to open `{archive}`"))?;
    let mut archive = zip::ZipArchive::new(file)?;
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
        let out = target.join(rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&out)?;
        } else {
            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut output = File::create(&out)?;
            std::io::copy(&mut entry, &mut output)?;
        }
    }
    Ok(())
}

fn copy_dir_all(source: &Utf8Path, target: &Utf8Path) -> Result<()> {
    for entry in WalkDir::new(source).into_iter().filter_map(Result::ok) {
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            anyhow::anyhow!("source path is not valid UTF-8: {}", path.display())
        })?;
        let rel = path.strip_prefix(source)?;
        if rel.as_str().is_empty() {
            continue;
        }
        let dest = target.join(rel);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dest)?;
        } else {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(&path, &dest)?;
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
                .map(|command| format!("{} {:?}", command.bin, command.args))
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    println!(
        "  containers: enabled={}, images={}, on_missing={}",
        permissions.containers.enabled,
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
                .map(|(name, secret)| format!("{name} ({})", secret.env))
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
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        anyhow::bail!("operation cancelled")
    }
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

fn package(dir: &Utf8Path, output: &Utf8Path) -> Result<()> {
    let file = File::create(output)?;
    let mut zip = zip::ZipWriter::new(file);
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    let mut buffer = Vec::new();
    for entry in WalkDir::new(dir).into_iter().filter_map(Result::ok) {
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            anyhow::anyhow!("package path is not valid UTF-8: {}", path.display())
        })?;
        let name = path.strip_prefix(dir)?.to_string();
        if name.is_empty() {
            continue;
        }
        if entry.file_type().is_dir() {
            zip.add_directory(name, options)?;
        } else {
            zip.start_file(name, options)?;
            let mut file = File::open(path)?;
            file.read_to_end(&mut buffer)?;
            zip.write_all(&buffer)?;
            buffer.clear();
        }
    }
    zip.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        std::fs::write(
            run_meta_dir(&run_dir).join("journal.jsonl"),
            [
                json!({
                    "t": "run_started",
                    "ts": "1970-01-01T00:00:00Z",
                    "generator": "gc-fixture",
                    "generator_path": generator_dir,
                    "inputs": {}
                })
                .to_string(),
                json!({
                    "t": "run_finished",
                    "ts": "1970-01-01T00:00:01Z",
                    "status": "success"
                })
                .to_string(),
            ]
            .join("\n")
                + "\n",
        )
        .expect("journal should be written");

        gc_runs_impl(&runs_dir, 50, true, false).expect("silent GC should run");

        assert!(!run_dir.exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn replay_seed_is_read_from_original_llm_call() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-replay-seed-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(run_meta_dir(&root))
            .expect("run metadata directory should be created");
        std::fs::write(
            run_meta_dir(&root).join("journal.jsonl"),
            [
                json!({ "t": "run_started", "ts": "2026-01-01T00:00:00Z" }).to_string(),
                json!({ "t": "llm_call", "seed": 12345_u64 }).to_string(),
            ]
            .join("\n")
                + "\n",
        )
        .expect("journal should be written");

        let seed = replay_seed_from_journal(&root).expect("seed should be read");

        assert_eq!(seed, 12345);
        let _ = std::fs::remove_dir_all(&root);
    }
}
