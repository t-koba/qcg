use anyhow::{Context, Result};
use api::{ForkRun, ForkStatePatch, RunStatus};
use camino::Utf8PathBuf;
use clap::Parser;
use contract::Contract;
use serde_json::json;
use service::{DirectRun, LocalService, step_param_schemas_markdown};
use std::collections::BTreeSet;

mod cli;
mod http;
mod registry;

use cli::args::{Cli, Command, DocsCommand, RegistryCommand, RunsCommand, SkillCommand};
use cli::eval::run_eval;
use cli::gc::{auto_gc_runs, gc_runs};
use cli::inputs::{load_answers, load_confirmations, load_inputs};
use cli::install::{InstallVerification, install, sign_package, uninstall};
use cli::new::new_generator;
use cli::package_cmd::{package, sha256_file};
use cli::plan::print_run_plan;
use cli::replay::{ReplayRequest, export_run_trace, replay_run};
use cli::runs_cli::{directory_scan_limit, list_runs, show_costs, show_run};
use cli::setup::{bundled_generators_root, init_tracing};
use service::app_registry_with_providers as app_registry;

// Windows main threads default to a 1 MiB stack, which overflows while clap
// builds the CLI command tree. Run everything on a thread with an explicit
// 8 MiB stack so behaviour is identical on every platform.
const MAIN_THREAD_STACK_BYTES: usize = 8 * 1024 * 1024;

/// Frozen startup deployment inputs shared by local execution consumers.
#[derive(Clone)]
pub(crate) struct CliDeployment {
    providers_path: Option<Utf8PathBuf>,
    container_timeouts: policy::ContainerTimeouts,
    /// Retention budget every sweep path applies, resolved once.
    retention: policy::RetentionPolicy,
    /// Per-run step ceiling frozen with the other deployment inputs. The
    /// service-owned resolver checks it on every CLI startup; `None` keeps
    /// the engine default.
    max_total_steps: Option<usize>,
}

impl CliDeployment {
    fn from_env(providers_path: Option<Utf8PathBuf>) -> Result<Self> {
        let container_timeouts =
            policy::ContainerTimeouts::from_env().map_err(anyhow::Error::msg)?;
        let retention = policy::RetentionPolicy::from_env()
            .map_err(|detail| anyhow::anyhow!("invalid GC configuration: {detail}"))?;
        let max_total_steps = service::ServiceDeploymentPolicy::default()
            .with_max_total_steps_env()
            .map_err(anyhow::Error::msg)?
            .max_total_steps;
        Ok(Self {
            providers_path,
            container_timeouts,
            retention,
            max_total_steps,
        })
    }

    fn defaults(providers_path: Option<Utf8PathBuf>) -> Self {
        Self {
            providers_path,
            container_timeouts: policy::ContainerTimeouts::default(),
            retention: policy::RetentionPolicy::default(),
            max_total_steps: None,
        }
    }
}

/// CLI-owned service construction (E04): every CLI command builds the
/// service through the policy constructor with explicit CLI defaults.
/// No production path may use the test-only `LocalService::new`.
pub(crate) fn local_cli_service(
    generators_dir: Utf8PathBuf,
    runs_dir: Utf8PathBuf,
    deployment: CliDeployment,
) -> Result<LocalService> {
    Ok(LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        deployment.providers_path,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        service::RunStoreMode::Exclusive,
        service::ServiceDeploymentPolicy {
            container_timeouts: deployment.container_timeouts,
            gc_keep: deployment.retention.keep,
            gc_keep_failed: deployment.retention.keep_failed,
            max_directory_scan_entries: deployment.retention.max_scan_entries,
            max_total_steps: deployment.max_total_steps,
            ..service::ServiceDeploymentPolicy::default()
        }
        .with_scheduling_env()
        .and_then(service::ServiceDeploymentPolicy::with_live_event_channel_capacity_env)
        .map_err(anyhow::Error::msg)?,
    )?)
}

/// Installs `[operational_limits]` before any contract is validated so a
/// tightened or raised ceiling is already effective for `Contract::load`.
/// Missing registry with no explicit path means built-ins (defaults).
fn preload_operational_limits_for_contracts(
    providers_path: Option<&camino::Utf8Path>,
) -> Result<()> {
    service::preload_operational_limits(providers_path)
        .map_err(|error| anyhow::anyhow!("invalid providers registry: {error}"))
}

/// Shared serve boot for `serve` and `dev`. Resolve once before
/// binding (E04 single freeze): validation and serving observe identical
/// values; a refused boot never occupies the port.
pub(crate) async fn serve_with_config(
    config: server::ServerConfig,
    bind: &str,
    port: u16,
    container_timeouts: policy::ContainerTimeouts,
) -> Result<()> {
    let policy = server::resolve_server_policy_with_container_timeouts(&config, container_timeouts)
        .map_err(|detail| anyhow::anyhow!("invalid server configuration: {detail}"))?;
    let addr: std::net::SocketAddr = format!("{bind}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;
    let service = policy::default_service_name();
    println!("{service} listening on http://{actual_addr}");
    server::serve_with_resolved_policy(policy, config, listener).await?;
    Ok(())
}

fn main() -> Result<std::process::ExitCode> {
    std::thread::Builder::new()
        .name("main".to_owned())
        .stack_size(MAIN_THREAD_STACK_BYTES)
        .spawn(run)
        .expect("failed to spawn main thread")
        .join()
        .expect("main thread panicked")
}

fn run() -> Result<std::process::ExitCode> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build async runtime")?
        .block_on(run_async())
}

async fn run_async() -> Result<std::process::ExitCode> {
    let cli = Cli::parse();
    init_tracing(cli.verbose, cli.log_format);
    let providers_path = cli.providers.clone();
    match cli.command {
        Command::Auth { command } => {
            cli::auth::run(command, providers_path.as_deref()).await?;
        }
        Command::New { path, id, force } => {
            new_generator(&path, id.as_deref(), force)?;
        }
        Command::Validate { path, json } => {
            if !json {
                preload_operational_limits_for_contracts(providers_path.as_deref())?;
                let contract = Contract::load(&path)?;
                app_registry(providers_path.as_deref())?.validate_contract(&contract)?;
                println!(
                    "valid: {}@{} ({})",
                    contract.manifest.generator.id,
                    contract.manifest.generator.version,
                    contract.sha256
                );
                return Ok(std::process::ExitCode::SUCCESS);
            }
            let result = (|| {
                preload_operational_limits_for_contracts(providers_path.as_deref())?;
                let contract = Contract::load(&path)?;
                app_registry(providers_path.as_deref())?.validate_contract(&contract)?;
                Ok::<_, anyhow::Error>(contract)
            })();
            match result {
                Ok(contract) => println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "valid": true,
                        "generator": format!(
                            "{}@{}",
                            contract.manifest.generator.id,
                            contract.manifest.generator.version
                        ),
                        "contract_sha256": contract.sha256,
                    }))?
                ),
                Err(error) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "valid": false,
                            "error": format!("{error:#}"),
                        }))?
                    );
                    // Report through the return code, never
                    // process::exit: the machine-readable verdict above
                    // is the report, and the exit status carries failure.
                    return Ok(std::process::ExitCode::FAILURE);
                }
            }
        }
        Command::Run {
            generator,
            inputs,
            inputs_file,
            input_files,
            answers,
            confirms,
            confirmations_file,
            output,
            yes,
            json,
            max_inputs_file_bytes,
            plan,
            diff,
        } => {
            if diff && !plan {
                anyhow::bail!("--diff requires --plan");
            }
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
            let contract = Contract::load(&generator)?;
            let input_runtime = contract.manifest.runtime.clone();
            let inputs = load_inputs(
                inputs,
                inputs_file,
                input_files,
                &input_runtime,
                max_inputs_file_bytes,
            )?;
            let answers = load_answers(answers)?;
            let confirmations = load_confirmations(confirms, confirmations_file)?;
            if plan {
                print_run_plan(&contract, &inputs, &answers, &confirmations, json, diff)?;
                return Ok(std::process::ExitCode::SUCCESS);
            }
            let runs_dir = Utf8PathBuf::from(".data/runs");
            let deployment = CliDeployment::from_env(providers_path.clone())?;
            let service =
                local_cli_service(Utf8PathBuf::new(), runs_dir.clone(), deployment.clone())?;
            auto_gc_runs(&runs_dir, &deployment.retention)?;
            let cancellation = tokio_util::sync::CancellationToken::new();
            let run = DirectRun {
                generator_path: generator,
                inputs,
                output_dir: output,
                json_events: false,
                interactive: !yes,
                answers,
                confirmations,
                llm_seed_override: None,
                cancellation: cancellation.clone(),
            };
            cli::signals::with_run_signals(cancellation, async {
                if json {
                    let result = service.run_generator_path_with_events(run).await?;
                    for event in result.events {
                        println!("{}", serde_json::to_string(&event)?);
                    }
                } else {
                    let manifest = service.run_generator_path(run).await?;
                    println!("{}", serde_json::to_string_pretty(&manifest)?);
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        }
        Command::Eval {
            generator,
            suite,
            output,
            runs_dir,
            baseline,
            json,
        } => {
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
            let deployment = CliDeployment::from_env(providers_path.clone())?;
            run_eval(
                generator,
                &suite,
                &output,
                runs_dir,
                deployment.clone(),
                baseline.as_deref(),
                json,
            )
            .await?;
        }
        Command::List { generators_dir } => {
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
            let scan_limit = directory_scan_limit()?;
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
                    if scanned > scan_limit {
                        anyhow::bail!("generator roots contain more than {scan_limit} entries");
                    }
                    let entry = entry?;
                    let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                        anyhow::anyhow!("path is not valid UTF-8: {}", path.display())
                    })?;
                    if path.join(contract::MANIFEST_FILE).exists() {
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
        Command::Models { refresh, json } => {
            let deployment = CliDeployment::defaults(providers_path.clone());
            let service = local_cli_service(
                Utf8PathBuf::new(),
                Utf8PathBuf::from(".data/runs"),
                deployment,
            )?;
            let catalog = service.llm_catalog(refresh).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&catalog)?);
            } else {
                print_llm_catalog(&catalog);
            }
        }
        Command::Docs { command } => match command {
            DocsCommand::StepSchemas => {
                print!("{}", step_param_schemas_markdown()?);
            }
            DocsCommand::RunEvents => {
                print!("{}", api::run_event_reference_markdown());
            }
            DocsCommand::Openapi => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&api::openapi_document(env!(
                        "CARGO_PKG_VERSION"
                    )))?
                );
            }
        },
        Command::Runs { command } => match command {
            RunsCommand::List {
                runs_dir,
                state,
                generator,
            } => list_runs(&runs_dir, state.as_deref(), generator.as_deref())?,
            RunsCommand::Show {
                id,
                runs_dir,
                json,
                diagnose,
            } => show_run(&runs_dir, &id, json, diagnose)?,
            RunsCommand::Costs {
                runs_dir,
                state,
                generator,
                by_model,
                json,
            } => show_costs(&runs_dir, &state, &generator, by_model, json)?,
            RunsCommand::Replay {
                id,
                generator,
                runs_dir,
                output,
                reuse_seed,
                answers,
                confirms,
                confirmations_file,
                json,
            } => {
                let deployment = CliDeployment::from_env(providers_path.clone())?;
                replay_run(
                    ReplayRequest {
                        runs_dir: runs_dir.clone(),
                        id: id.clone(),
                        generator: generator.clone(),
                        output: output.clone(),
                        reuse_seed,
                        answers: load_answers(answers)?,
                        confirmations: load_confirmations(confirms, confirmations_file)?,
                        json_output: json,
                    },
                    deployment,
                )
                .await?
            }
            RunsCommand::Continue { id, runs_dir, json } => {
                let deployment = CliDeployment::from_env(providers_path.clone())?;
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, deployment)?;
                match service.continue_run(&id).await {
                    Ok(child_id) => {
                        if json {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&json!({
                                    "child_run_id": child_id,
                                }))?
                            );
                        } else {
                            println!("continued {id} -> {child_id}");
                        }
                    }
                    Err(refusal) => anyhow::bail!("continuation refused: {}", refusal.reason()),
                }
            }
            RunsCommand::Fork {
                id,
                at_seq,
                reuse,
                state_patch,
                answers,
                confirms,
                confirmations_file,
                runs_dir,
                json,
            } => {
                let state_patch = match state_patch {
                    Some(path) => {
                        serde_json::from_slice::<ForkStatePatch>(&files::read_bounded(&path, None)?)
                            .with_context(|| format!("failed to parse state patch `{path}`"))?
                    }
                    None => ForkStatePatch::default(),
                };
                let deployment = CliDeployment::from_env(providers_path.clone())?;
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, deployment)?;
                let fork_id = service
                    .fork_run(
                        &id,
                        ForkRun {
                            at_seq: at_seq.unwrap_or(0),
                            reuse: reuse.then_some(api::ForkReuse::Matching),
                            state_patch,
                            answers: load_answers(answers)?,
                            confirmations: load_confirmations(confirms, confirmations_file)?,
                            ..Default::default()
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
                    println!(
                        "forked {id}@{} -> {fork_id} ({})",
                        at_seq.map_or_else(|| "start".to_string(), |seq| seq.to_string()),
                        snapshot.state
                    );
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
                keep_failed,
                delete,
            } => {
                // The manual sweep resolves the same policy the deployment
                // uses; an explicit count overrides it.
                let policy = policy::RetentionPolicy::from_env()
                    .map_err(|detail| anyhow::anyhow!("invalid GC configuration: {detail}"))?;
                gc_runs(&runs_dir, &policy, keep, keep_failed, delete)?
            }
            RunsCommand::Invalidate {
                id,
                runs_dir,
                reason,
                actor_label,
                json,
            } => {
                let deployment = CliDeployment::defaults(providers_path.clone());
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, deployment)?;
                let record = service
                    .invalidate_run(
                        &id,
                        api::InvalidateRun {
                            reason,
                            actor_label,
                        },
                    )
                    .await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&record)?);
                } else {
                    println!(
                        "invalidated {id}: {} ({} at {})",
                        record.reason, record.actor_label, record.invalidated_at
                    );
                }
            }
            RunsCommand::Delete { id, runs_dir, json } => {
                let deployment = CliDeployment::defaults(providers_path.clone());
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, deployment)?;
                service.delete_run(&id).await?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "deleted": id,
                        }))?
                    );
                } else {
                    println!("deleted {id}");
                }
            }
            RunsCommand::Export {
                id,
                runs_dir,
                output,
            } => {
                let deployment = CliDeployment::defaults(providers_path.clone());
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, deployment)?;
                let parts = service
                    .run_bundle_parts(&id, &service::ArtifactZipLimits::default())
                    .await?;
                let output =
                    output.unwrap_or_else(|| Utf8PathBuf::from(format!("{id}-bundle.zip")));
                parts.validate_output_path(&output)?;
                let file = std::fs::File::create(&output)
                    .with_context(|| format!("failed to create bundle `{output}`"))?;
                service::write_run_bundle_stream_with_limits(
                    &parts,
                    file,
                    &service::ArtifactZipLimits::default(),
                )?;
                println!("exported {id} -> {output}");
            }
        },
        Command::Package {
            dir,
            output,
            signing_key,
            max_entries,
            max_bytes,
            max_metadata_bytes,
        } => {
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
            let output = output.unwrap_or_else(|| {
                Utf8PathBuf::from(format!("{}.pkg", dir.file_name().unwrap_or("generator")))
            });
            let defaults = service::PackageLimits::default();
            let limits = service::PackageLimits {
                max_entries: max_entries.or(defaults.max_entries),
                max_bytes: max_bytes.or(defaults.max_bytes),
                max_metadata_bytes,
                max_archive_bytes: None,
            };
            package(&dir, &output, &limits)?;
            println!("sha256 {}", sha256_file(&output, limits.max_archive_bytes)?);
            if let Some(signing_key) = signing_key {
                let bytes = files::read_bounded(
                    &output,
                    limits
                        .max_archive_bytes
                        .map(|limit| {
                            usize::try_from(limit).map_err(|error| {
                                anyhow::anyhow!(
                                    "package archive limit is not representable: {error}"
                                )
                            })
                        })
                        .transpose()?,
                )?;
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
            max_entries,
            max_bytes,
            max_archive_bytes,
            max_metadata_bytes,
        } => {
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
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
                &service::PackageLimits {
                    max_entries: max_entries.or(service::PackageLimits::default().max_entries),
                    max_bytes: max_bytes.or(service::PackageLimits::default().max_bytes),
                    max_metadata_bytes,
                    max_archive_bytes: max_archive_bytes
                        .or(service::PackageLimits::default().max_archive_bytes),
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
        Command::Registry { command } => match command {
            RegistryCommand::Add { name, url } => {
                if name.trim().is_empty() || name.contains('/') || name.contains('\\') {
                    anyhow::bail!("registry name `{name}` must be a plain name");
                }
                if !url.starts_with("file://") && !url.starts_with("https://") {
                    anyhow::bail!("registry url `{url}` must use file:// or https://");
                }
                // Fail fast on unreachable or malformed indexes.
                registry::fetch_index(&url).await?;
                let home = registry::home_dir()?;
                let mut config = registry::load_registries(&home)?;
                config.registries.insert(name.clone(), url.clone());
                registry::save_registries(&home, &config)?;
                println!("registry `{name}` added ({url})");
            }
            RegistryCommand::Remove { name } => {
                let home = registry::home_dir()?;
                let mut config = registry::load_registries(&home)?;
                if config.registries.remove(&name).is_none() {
                    anyhow::bail!("registry `{name}` is not configured");
                }
                registry::save_registries(&home, &config)?;
                println!("registry `{name}` removed");
            }
            RegistryCommand::List => {
                let home = registry::home_dir()?;
                let config = registry::load_registries(&home)?;
                if config.registries.is_empty() {
                    println!("no registries configured");
                }
                for (name, url) in &config.registries {
                    println!("{name} {url}");
                }
            }
        },
        Command::Skill { command } => match command {
            SkillCommand::Validate {
                path,
                library,
                json,
            } => return cli::skill::validate_skill(&path, library, json),
        },
        Command::Search { query } => {
            let home = registry::home_dir()?;
            let config = registry::load_registries(&home)?;
            let needle = query.to_lowercase();
            let mut hits = 0;
            for (registry, url) in &config.registries {
                let index = registry::fetch_index(url).await?;
                for entry in index.packages_for_search() {
                    if entry.id.to_lowercase().contains(&needle)
                        || entry.description.to_lowercase().contains(&needle)
                    {
                        println!(
                            "{}@{} {} [{}]",
                            entry.id, entry.version, entry.description, registry
                        );
                        hits += 1;
                    }
                }
            }
            if hits == 0 {
                println!("no packages match `{query}`");
            }
        }
        Command::Serve {
            bind,
            port,
            generators_dir,
            runs_dir,
            max_active_runs,
            max_tracked_runs,
            max_total_steps,
            run_store,
            cors_origins,
            api_token,
            max_request_bytes,
            max_artifact_bytes,
            max_artifact_response_bytes,
            max_artifact_entries,
            max_asset_bytes,
        } => {
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
            let container_timeouts =
                policy::ContainerTimeouts::from_env().map_err(anyhow::Error::msg)?;
            let api_token = resolve_api_token(api_token)?;
            let config = server::ServerConfig {
                providers_path,
                extra_generators_dirs: bundled_generators_root()
                    .filter(|bundled| bundled != &generators_dir)
                    .into_iter()
                    .collect(),
                generators_dir,
                runs_dir,
                max_active_runs,
                max_tracked_runs,
                max_total_steps,
                run_store_mode: run_store.into(),
                cors_origins,
                api_token,
                max_request_bytes,
                max_artifact_bytes,
                max_artifact_response_bytes,
                max_artifact_entries,
                max_asset_bytes,
            };
            serve_with_config(config, &bind, port, container_timeouts).await?;
        }
        Command::Dev {
            bind,
            port,
            generators_dir,
            runs_dir,
            max_active_runs,
            watch_interval_ms,
            eval,
        } => {
            preload_operational_limits_for_contracts(providers_path.as_deref())?;
            let container_timeouts =
                policy::ContainerTimeouts::from_env().map_err(anyhow::Error::msg)?;
            let retention = policy::RetentionPolicy::from_env()
                .map_err(|detail| anyhow::anyhow!("invalid GC configuration: {detail}"))?;
            let config = server::ServerConfig {
                providers_path,
                extra_generators_dirs: bundled_generators_root()
                    .filter(|bundled| bundled != &generators_dir)
                    .into_iter()
                    .collect(),
                generators_dir,
                runs_dir,
                max_active_runs,
                max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
                max_total_steps: None,
                run_store_mode: service::RunStoreMode::Exclusive,
                cors_origins: Vec::new(),
                api_token: None,
                max_request_bytes: None,
                max_artifact_bytes: None,
                max_artifact_response_bytes: None,
                max_artifact_entries: None,
                max_asset_bytes: None,
            };
            cli::dev::run_dev(
                config,
                bind,
                port,
                watch_interval_ms,
                eval,
                container_timeouts,
                retention,
            )
            .await?;
        }
    }
    Ok(std::process::ExitCode::SUCCESS)
}

fn command_hint(args: &str) -> String {
    policy::binary_stem().map_or_else(|| args.to_owned(), |stem| format!("{stem} {args}"))
}

/// Resolves the instance bearer token: an explicit flag or `API_TOKEN`
/// wins, then `API_TOKEN_FILE` (the only documented file source). An
/// unreadable or empty file fails closed instead of booting unauthenticated.
fn resolve_api_token(explicit: Option<String>) -> anyhow::Result<Option<String>> {
    if explicit.is_some() {
        return Ok(explicit);
    }
    let Some(path) = std::env::var_os("API_TOKEN_FILE") else {
        return Ok(None);
    };
    let path = camino::Utf8PathBuf::from_path_buf(std::path::PathBuf::from(path))
        .map_err(|path| anyhow::anyhow!("API_TOKEN_FILE is not valid UTF-8: {}", path.display()))?;
    let bytes = files::read_bounded(&path, Some(policy::MAX_CREDENTIAL_FILE_BYTES as usize))
        .map_err(|error| anyhow::anyhow!("failed to read API_TOKEN_FILE `{path}`: {error}"))?;
    let token = String::from_utf8(bytes)
        .map_err(|error| anyhow::anyhow!("API_TOKEN_FILE `{path}` is not UTF-8: {error}"))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("API_TOKEN_FILE `{path}` is empty");
    }
    Ok(Some(token))
}

fn format_catalog_cost_tiers(tiers: &[api::LlmCatalogCostTier]) -> String {
    if tiers.is_empty() {
        return "-".into();
    }
    tiers
        .iter()
        .map(|tier| {
            format!(
                ">={}:{}/{}/{}/{}",
                tier.threshold_tokens,
                catalog_price(tier.input_cost_per_million_usd),
                catalog_price(tier.cache_read_input_cost_per_million_usd),
                catalog_price(tier.cache_creation_input_cost_per_million_usd),
                catalog_price(tier.output_cost_per_million_usd),
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn catalog_price(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn print_llm_catalog(catalog: &api::LlmCatalogResponse) {
    println!(
        "provider\tmodel\tenabled\tavailable\tefforts\tinput\toutput\tcontext\tsource\twarning\tcost_tiers(input/read/create/output USD/1M)"
    );
    for provider in &catalog.providers {
        for model in &provider.models {
            let efforts = if model.reasoning_effort.is_empty() {
                "-".to_string()
            } else {
                model.reasoning_effort.join(",")
            };
            let context = model
                .context_tokens
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                provider.id,
                model.id,
                model.enabled,
                provider.available,
                efforts,
                catalog_price(model.input_cost_per_million_usd),
                catalog_price(model.output_cost_per_million_usd),
                context,
                model.source,
                model.warning.as_deref().unwrap_or("-"),
                format_catalog_cost_tiers(&model.cost_tiers)
            );
        }
        if let Some(error) = &provider.error {
            eprintln!("provider {}: {error}", provider.id);
        }
    }
    if catalog.stale {
        eprintln!(
            "catalog metadata is stale; run `{}`",
            command_hint("models --refresh")
        );
    }
    for source in &catalog.sources {
        if let Some(error) = &source.error {
            eprintln!("catalog source {}: {error}", source.location);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::eval::{
        EvalCase, EvalCaseReport, EvalSuite, percentile_u64, summarize_eval_cases,
    };
    use crate::cli::install::{CleanupPaths, commit_install, verify_package_bytes};
    use crate::cli::plan::{plan_command_allowed, read_bounded_confirmation};
    use crate::cli::replay::replay_seed_from_journal;
    use crate::cli::runs_cli::{find_pricing, format_usd};
    use aws_lc_rs::rand::SystemRandom;
    use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
    use policy::MAX_CONFIRM_INPUT_BYTES;
    use service::run_meta_dir;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::fs::File;
    use uuid::Uuid;

    #[test]
    fn command_hints_use_the_binary_not_the_service_name() {
        if env_process::isolated() {
            return;
        }
        // SAFETY: the isolated child process owns this environment.
        unsafe {
            std::env::set_var("SERVICE_NAME", "llm-gateway");
        }
        let hint = command_hint("models --refresh");
        unsafe {
            std::env::remove_var("SERVICE_NAME");
        }
        assert!(
            !hint.contains("llm-gateway"),
            "a hint must not name the service identity: {hint}"
        );
        let stem = policy::binary_stem().expect("test binary has a stem");
        assert_eq!(hint, format!("{stem} models --refresh"));
    }

    fn journal_events(runs: &Utf8PathBuf, run_id: &str) -> Vec<serde_json::Value> {
        let journal = runs.join(run_id).join("meta/journal.jsonl");
        let text = std::fs::read_to_string(&journal)
            .unwrap_or_else(|error| panic!("journal {} should read: {error}", journal));
        text.lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    fn event<'a>(events: &'a [serde_json::Value], kind: &str) -> &'a serde_json::Value {
        events
            .iter()
            .find(|value| value.get("t").and_then(serde_json::Value::as_str) == Some(kind))
            .unwrap_or_else(|| panic!("journal should contain a {kind} event"))
    }

    /// Defensive journal snapshot for a timed-out run. Unlike
    /// `journal_events`, this must never panic: the file may be missing or
    /// mid-write at the moment the bound expires.
    fn journal_timeout_diagnostics(runs: &Utf8PathBuf, run_id: &str) -> String {
        let journal = runs.join(run_id).join("meta/journal.jsonl");
        let mut diag = format!("run_id={run_id}; runs={runs}; journal={journal}");
        match std::fs::read(&journal) {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                let events: Vec<serde_json::Value> = text
                    .lines()
                    .filter_map(|line| serde_json::from_str(line).ok())
                    .collect();
                let types: Vec<&str> = events
                    .iter()
                    .filter_map(|event| event.get("t").and_then(serde_json::Value::as_str))
                    .collect();
                let last_types = types
                    .iter()
                    .rev()
                    .take(20)
                    .rev()
                    .copied()
                    .collect::<Vec<_>>();
                let tail = String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(4000)..]);
                let parsed_events = events.len();
                diag.push_str(&format!(
                    "; parsed_events={parsed_events}; last_event_types={last_types:?}; journal_tail_last_4000_bytes={tail}"
                ));
            }
            Err(error) => diag.push_str(&format!("; journal_read_error={error}")),
        }
        diag
    }

    async fn wait_for_terminal_events(runs: Utf8PathBuf, run_id: String) -> Vec<serde_json::Value> {
        match tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let events = journal_events(&runs, &run_id);
                if events.iter().any(|value| {
                    value.get("t").and_then(serde_json::Value::as_str) == Some("run_finished")
                }) {
                    return events;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        {
            Ok(events) => events,
            Err(_) => {
                let diag = journal_timeout_diagnostics(&runs, &run_id);
                panic!(
                    "the run should reach a terminal journal event within the bound; diagnostics: {diag}"
                )
            }
        }
    }

    /// Fixed check for the accepted `MAX_TOTAL_STEPS` finding: the CLI's
    /// startup resolver and the serve flag both freeze one non-default
    /// ceiling, and both real execution paths journal and enforce it.
    ///
    /// Executed here: `CliDeployment::from_env` (the `qcg run` startup
    /// resolver), clap's real `qcg serve` argument/env binding,
    /// `local_cli_service` plus the real direct
    /// `run_generator_path_with_events`, real `resolve_server_policy` + an
    /// in-process `serve` HTTP listener, and its `POST /api/runs` admission.
    /// Not executed: the `qcg` executable; `tests/max_total_steps.rs`
    /// covers the literal binary separately.
    #[tokio::test]
    async fn max_total_steps_ceiling_matches_on_cli_and_serve_boots() {
        if env_process::isolated() {
            return;
        }
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("max-steps-parity-{}", Uuid::now_v7()));
        let generators = root.join("generators");
        let cli_runs = root.join("cli-runs");
        let serve_runs = root.join("serve-runs");
        std::fs::create_dir_all(generators.join("tiny-three-steps"))
            .expect("generator directory should create");
        // Three deterministic steps against a ceiling of two: both paths
        // must record the same effective budget and refuse the third step.
        std::fs::write(
            generators
                .join("tiny-three-steps")
                .join(contract::MANIFEST_FILE),
            r#"[generator]
id = "tiny-three-steps"
name = "Tiny Three Steps"
version = "0.1.0"
[[inputs.stages]]
id = "basic"
[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
[[flow]]
id = "one"
type = "write"
[flow.params]
content = "one"
output_file = "one.txt"
[[flow]]
id = "two"
type = "write"
[flow.params]
content = "two"
output_file = "two.txt"
[[flow]]
id = "three"
type = "write"
[flow.params]
content = "three"
output_file = "three.txt"
"#,
        )
        .expect("fixture manifest should write");

        let variable = "MAX_TOTAL_STEPS";
        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::remove_var(variable) };
        let default_cli = CliDeployment::from_env(None).expect("unset ceiling should boot via CLI");
        assert_eq!(default_cli.max_total_steps, None);
        let mut config = server::ServerConfig {
            generators_dir: generators.clone(),
            providers_path: None,
            extra_generators_dirs: Vec::new(),
            runs_dir: serve_runs.clone(),
            max_active_runs: 1,
            max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
            run_store_mode: service::RunStoreMode::Exclusive,
            cors_origins: Vec::new(),
            api_token: None,
            max_request_bytes: None,
            max_artifact_bytes: None,
            max_artifact_response_bytes: None,
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: None,
        };
        let default_server = server::resolve_server_policy(&config).expect("unset should resolve");
        assert_eq!(default_server.max_total_steps, None);

        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::set_var(variable, "2") };
        let deployment = CliDeployment::from_env(None).expect("valid ceiling should boot via CLI");
        assert_eq!(deployment.max_total_steps, Some(2));
        // The serve ceiling comes from the real Serve clap/env binding, not
        // from the CLI resolver: removing `env = "MAX_TOTAL_STEPS"` from the
        // Serve arg must leave this parse at None and fail the parity check.
        let Command::Serve {
            max_total_steps: serve_max_total_steps,
            ..
        } = Cli::try_parse_from(["qcg", "serve"])
            .expect("serve with a valid ceiling should parse")
            .command
        else {
            panic!("bare serve should parse as the Serve command");
        };
        assert_eq!(serve_max_total_steps, Some(2));
        config.max_total_steps = serve_max_total_steps;
        let server_policy =
            server::resolve_server_policy(&config).expect("valid ceiling should resolve");
        assert_eq!(server_policy.max_total_steps, Some(2));

        let request = api::StartRun {
            generator_id: "tiny-three-steps".into(),
            ..Default::default()
        };
        let cli_service = local_cli_service(generators.clone(), cli_runs.clone(), deployment)
            .expect("CLI service should construct");
        let cli_output = cli_runs.join("tiny-three-steps-direct");
        let cli_meta = service::direct_run_meta_dir(&cli_output);
        let cli_run_dir = cli_meta
            .parent()
            .expect("direct run meta should have a run dir");
        let cli_runs_dir = cli_run_dir
            .parent()
            .expect("direct run dir should have a store")
            .to_owned();
        let cli_id = cli_run_dir
            .file_name()
            .expect("direct run dir should have an id")
            .to_owned();
        // The literal CLI run path: a DirectRun through the service's direct
        // pipeline, not an API admission standing in for it.
        let cli_direct_error = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            cli_service
                .run_generator_path_with_events(DirectRun {
                    generator_path: generators.join("tiny-three-steps"),
                    inputs: BTreeMap::new(),
                    output_dir: cli_output,
                    json_events: false,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    llm_seed_override: None,
                    cancellation: tokio_util::sync::CancellationToken::new(),
                })
                .await
        })
        .await
        .expect("the direct run should finish within the bound")
        .expect_err("the over-ceiling third step should refuse the direct run")
        .to_string();
        assert!(
            cli_direct_error.contains("global step budget exceeded: 2 >= 2"),
            "{cli_direct_error}"
        );
        drop(cli_service);

        // Real serve listener: the same policy resolution and service
        // construction the binary uses, with the flag carried by config.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have an address");
        let server_task = tokio::spawn(server::serve_with_resolved_policy(
            server_policy,
            config.clone(),
            listener,
        ));
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("HTTP client should build");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if client
                    .get(format!("http://{addr}/healthz"))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the serve listener should start within the bound");

        let detail: api::GeneratorDetail = client
            .get(format!("http://{addr}/api/generators/tiny-three-steps"))
            .send()
            .await
            .expect("generator detail should respond")
            .error_for_status()
            .expect("generator detail should succeed")
            .json()
            .await
            .expect("generator detail should deserialize");
        let response = client
            .post(format!("http://{addr}/api/runs"))
            .header(
                reqwest::header::HeaderName::from_static(policy::IDEMPOTENCY_HEADER),
                uuid::Uuid::now_v7().to_string(),
            )
            .header("x-qcg-package-sha256", &detail.package_sha256)
            .json(&request)
            .send()
            .await
            .expect("start request should respond");
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);

        let serve_id = std::fs::read_dir(&serve_runs)
            .expect("serve runs directory should exist")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .find(|name| name.starts_with("tiny-three-steps-"))
            .expect("the listener should have admitted one run");
        let cli_events = wait_for_terminal_events(cli_runs_dir, cli_id).await;
        let serve_events = wait_for_terminal_events(serve_runs.clone(), serve_id).await;
        // Tear the listener down only after both admitted runs have reached a
        // terminal journal event. Aborting the server drops `ResidentTasks`,
        // whose Drop aborts in-flight engine tasks; doing that while a run is
        // still executing strands it mid-step with no `run_finished`. Keep the
        // listener alive until both runs settle (Windows yields inside the
        // atomic write, so the window is observable there).
        server_task.abort();
        let _ = server_task.await;

        let expected_origin = format!(
            "service-ceiling:2 caps contract:{}",
            engine::RunOptions::default_max_total_steps()
        );
        for events in [&cli_events, &serve_events] {
            let queued = event(events, "run_queued");
            assert_eq!(queued["effective_max_total_steps"], 2);
            assert_eq!(queued["effective_policy_origin"], expected_origin);
            // The over-ceiling third step is refused at the same recorded
            // budget on both paths, not merely reported in the journal.
            let finished = event(events, "run_finished");
            assert_eq!(finished["status"], "failed");
            assert_eq!(
                finished["failures"][0]["failure"]["code"],
                "budget_exceeded"
            );
            assert_eq!(
                finished["failures"][0]["failure"]["exhaustion"]["allowed"],
                2
            );
        }
        assert_eq!(
            event(&cli_events, "run_queued")["effective_max_total_steps"],
            event(&serve_events, "run_queued")["effective_max_total_steps"],
            "both startup paths must journal the same effective ceiling"
        );
        assert_eq!(
            event(&cli_events, "run_finished")["failures"][0]["failure"],
            event(&serve_events, "run_finished")["failures"][0]["failure"],
            "both startup paths must refuse the over-ceiling request identically"
        );

        // Same startup boundary refuses an invalid ceiling on both paths.
        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::set_var(variable, "0") };
        let zero_cli = match CliDeployment::from_env(None) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("zero ceiling must refuse CLI startup"),
        };
        let Command::Serve {
            max_total_steps: zero_serve_max_total_steps,
            ..
        } = Cli::try_parse_from(["qcg", "serve"])
            .expect("serve with a zero ceiling should parse")
            .command
        else {
            panic!("bare serve should parse as the Serve command");
        };
        assert_eq!(zero_serve_max_total_steps, Some(0));
        let mut zero_config = config.clone();
        zero_config.max_total_steps = zero_serve_max_total_steps;
        let zero_serve = server::resolve_server_policy(&zero_config)
            .expect_err("zero ceiling must refuse serve startup");
        assert!(
            zero_cli.contains("MAX_TOTAL_STEPS must be greater than zero"),
            "{zero_cli}"
        );
        assert!(
            zero_serve.contains("MAX_TOTAL_STEPS must be greater than zero"),
            "{zero_serve}"
        );
        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::set_var(variable, "many") };
        let bad_cli = match CliDeployment::from_env(None) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("unparseable ceiling must refuse CLI startup"),
        };
        assert!(bad_cli.contains("MAX_TOTAL_STEPS"), "{bad_cli}");
        let bad_serve = Cli::try_parse_from(["qcg", "serve"])
            .expect_err("an unparseable serve ceiling must refuse clap parsing")
            .to_string();
        assert!(bad_serve.contains("MAX_TOTAL_STEPS"), "{bad_serve}");
        // SAFETY: the isolated child process owns this environment.
        unsafe { std::env::remove_var(variable) };
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Both startup paths must freeze the same live channel capacity into
    /// their service policy. Each path constructs its service through its
    /// own startup code, and the capacity is observed on a real broadcast
    /// channel built from that constructed service's effective policy.
    #[test]
    fn live_event_channel_capacity_matches_on_cli_and_server_boots() {
        if env_process::isolated() {
            return;
        }
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("live-event-capacity-{}", Uuid::now_v7()));
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let config = server::ServerConfig {
            generators_dir: generators.clone(),
            providers_path: None,
            extra_generators_dirs: Vec::new(),
            runs_dir: runs.clone(),
            max_active_runs: 1,
            max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
            run_store_mode: service::RunStoreMode::Exclusive,
            cors_origins: Vec::new(),
            api_token: None,
            max_request_bytes: None,
            max_artifact_bytes: None,
            max_artifact_response_bytes: None,
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: None,
        };
        let variable = "LIVE_EVENT_CHANNEL_CAPACITY";
        let set = |value: &str| {
            // SAFETY: the isolated child process owns this environment.
            unsafe { std::env::set_var(variable, value) };
        };
        let unset = || {
            // SAFETY: the isolated child process owns this environment.
            unsafe { std::env::remove_var(variable) };
        };

        unset();
        let default_cli = local_cli_service(
            generators.clone(),
            runs.clone(),
            CliDeployment::defaults(None),
        )
        .expect("default CLI boot should succeed");
        let default_cli_capacity = default_cli.live_event_channel_capacity_probe();
        assert_eq!(default_cli_capacity, policy::LIVE_EVENT_CHANNEL_CAPACITY);
        drop(default_cli);

        let default_server =
            server::resolve_server_policy(&config).expect("default should resolve");
        assert_eq!(
            default_server.live_event_channel_capacity,
            policy::LIVE_EVENT_CHANNEL_CAPACITY
        );
        let default_serve = server::build_service_from_resolved_policy(&default_server, &config)
            .expect("default serve boot should construct its service");
        let default_serve_capacity = default_serve.live_event_channel_capacity_probe();
        assert_eq!(default_serve_capacity, policy::LIVE_EVENT_CHANNEL_CAPACITY);
        assert_eq!(
            default_serve_capacity, default_cli_capacity,
            "both startup paths must observe the configured default"
        );
        drop(default_serve);

        set("1024");
        let cli = local_cli_service(generators, runs, CliDeployment::defaults(None))
            .expect("valid capacity should boot via CLI");
        let cli_capacity = cli.live_event_channel_capacity_probe();
        assert_eq!(cli_capacity, 1024);
        drop(cli);

        let server = server::resolve_server_policy(&config).expect("valid capacity should resolve");
        let serve = server::build_service_from_resolved_policy(&server, &config)
            .expect("valid capacity should construct the serve service");
        let serve_capacity = serve.live_event_channel_capacity_probe();
        assert_eq!(serve_capacity, 1024);
        assert_eq!(
            serve_capacity, cli_capacity,
            "both startup paths must freeze the same effective capacity"
        );
        drop(serve);

        for invalid in ["1", "65537", "not-a-number", ""] {
            set(invalid);
            let cli_error = local_cli_service(
                config.generators_dir.clone(),
                config.runs_dir.clone(),
                CliDeployment::defaults(None),
            )
            .expect_err("invalid capacity must refuse CLI startup")
            .to_string();
            let server_error = server::resolve_server_policy(&config)
                .expect_err("invalid capacity must refuse server startup");
            assert!(cli_error.contains(variable), "{cli_error}");
            assert!(server_error.contains(variable), "{server_error}");
        }
        unset();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(test)]
    mod env_process {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/test-support/environment.rs"
        ));
    }

    #[test]
    fn usd_formatting_and_pricing_lookup_cover_display_cases() {
        assert_eq!(format_usd(0), "$0.000000");
        assert_eq!(format_usd(1), "$0.000001");
        assert_eq!(format_usd(4231), "$0.004231");
        assert_eq!(format_usd(1_000_000), "$1.000000");

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("pricing-test-{}", Uuid::now_v7()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("package directory should be created");
        std::fs::write(
            root.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "pricing-fixture"
name = "Pricing Fixture"
version = "0.1.0"

[llm]
max_tokens = 1024
[llm.model]
provider = "main"
model = "large"
input_cost_per_million_usd = 2.5
output_cost_per_million_usd = 10.0

[[llm.models]]
provider = "alt"
model = "small"
input_cost_per_million_usd = 0.5

[[flow]]
id = "emit"
type = "write"
params = { content = "x", output_file = "x.txt" }
"#,
        )
        .expect("manifest should be written");
        let contract = Contract::load(&root).expect("fixture contract should load");
        assert_eq!(
            find_pricing(&contract, "main", "large"),
            Some((2.5, 2.5, 2.5, 10.0))
        );
        assert_eq!(find_pricing(&contract, "alt", "small"), None);
        assert_eq!(find_pricing(&contract, "missing", "large"), None);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn load_confirmations_parses_decisions() {
        // Q1: confirmation ids are 3-part content or 4-part invocation
        // scope; legacy 2-part ids fail closed.
        let digest = "a".repeat(64);
        let other_digest = "b".repeat(64);
        let parsed = load_confirmations(
            vec![
                format!("effect:command:{digest}=approve"),
                format!("other:command:{other_digest}=deny"),
            ],
            None,
        )
        .expect("valid decisions should parse");
        assert_eq!(parsed.get(&format!("effect:command:{digest}")), Some(&true));
        assert_eq!(
            parsed.get(&format!("other:command:{other_digest}")),
            Some(&false)
        );
        assert!(
            load_confirmations(vec![format!("effect:command:{digest}=maybe")], None)
                .expect_err("unknown decision must fail")
                .to_string()
                .contains("approve|deny")
        );
        assert!(
            load_confirmations(vec!["no-separator".to_string()], None)
                .expect_err("missing separator must fail")
                .to_string()
                .contains("ID=approve|deny")
        );
        assert!(
            load_confirmations(vec!["effect:command=approve".to_string()], None)
                .expect_err("legacy two-part id must fail")
                .to_string()
                .contains("invalid confirmation id")
        );
    }

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
    fn automatic_sweep_honors_the_journal_recorded_expiry_and_the_resolved_counts() {
        if env_process::isolated() {
            return;
        }
        // SAFETY: the isolated child process owns this environment.
        unsafe {
            std::env::set_var("AUTO_GC", "true");
        }

        fn write_run(run_dir: &Utf8PathBuf, retention_days: u64) {
            std::fs::create_dir_all(run_meta_dir(run_dir))
                .expect("run metadata directory should be created");
            let run_id = run_dir.file_name().expect("run should have a name");
            let journal = engine::JournalWriter::create(
                &run_meta_dir(run_dir).join("journal.jsonl"),
                run_id,
                false,
                None,
            )
            .expect("journal should be created");
            journal
                .event(
                    "run_started",
                    json!({
                        "generator": "gc-fixture",
                        "generator_path": "gc-fixture",
                        "contract_sha256": "fixture",
                        "inputs": {},
                        "resource_hashes": [],
                        "schema_version": 1,
                        "retention_days": retention_days,
                    }),
                )
                .expect("run should start");
            journal
                .event("run_finished", json!({ "status": "success" }))
                .expect("run should finish");
        }

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("cli-gc-test-{}", Uuid::now_v7()));
        let _ = std::fs::remove_dir_all(&root);
        let runs_dir = root.join("runs");
        let expired = runs_dir.join("expired-run");
        let fresh = runs_dir.join("fresh-run");
        write_run(&expired, 0);
        write_run(&fresh, 365);
        // The journal decides expiry; the counts only decide what survives
        // inside the budget.
        let policy = policy::RetentionPolicy {
            keep: 1,
            keep_failed: 0,
            max_scan_entries: 16,
        };

        auto_gc_runs(&runs_dir, &policy).expect("the automatic sweep should run");

        assert!(
            !expired.exists(),
            "a run past its journal-recorded expiry is collected"
        );
        assert!(
            fresh.is_dir(),
            "a fresh run inside the keep budget survives"
        );
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
    fn failed_install_commit_cleanup_does_not_remove_user_source() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("cli-commit-failure-test-{}", Uuid::now_v7()));
        let source = root.join("source");
        let temporary = root.join("temporary");
        // Block parent creation with a regular file: `create_dir_all`
        // fails, so the commit fails while user sources must survive.
        let blocker = root.join("missing-parent");
        let target = blocker.join("generator");
        std::fs::create_dir_all(&source).expect("source directory should be created");
        std::fs::create_dir_all(&temporary).expect("temporary directory should be created");
        std::fs::write(source.join("source.txt"), "keep").expect("source file should be written");
        std::fs::write(temporary.join("new.txt"), "new").expect("new file should be written");
        std::fs::write(&blocker, "block").expect("parent blocker should be written");
        let temporary_guard = CleanupPaths::new(temporary.clone());

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
            .join(format!("cli-package-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let source = root.join("source");
        std::fs::create_dir_all(source.join("nested/empty"))
            .expect("empty directory should be created");
        std::fs::write(
            source.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "package-fixture"
name = "Package Fixture"
version = "0.1.0"

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
        let output = root.join("generator.pkg");

        package(&source, &output, &service::PackageLimits::default())
            .expect("package should be written");

        let file = File::open(&output).expect("package should open");
        let mut archive = zip::ZipArchive::new(file).expect("package should parse");
        assert!(
            archive
                .by_name("nested/")
                .expect("nested directory entry")
                .is_dir()
        );
        assert_eq!(
            archive
                .by_name("nested/")
                .expect("nested directory mode")
                .unix_mode()
                .expect("nested directory permissions")
                & 0o777,
            0o755
        );
        assert!(
            archive
                .by_name("nested/empty/")
                .expect("empty directory entry")
                .is_dir()
        );
        assert_eq!(
            archive
                .by_name("nested/empty/")
                .expect("empty directory mode")
                .unix_mode()
                .expect("empty directory permissions")
                & 0o777,
            0o755
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
            .by_name("SBOM.spdx.json")
            .expect("generated metadata entry");
        assert_eq!(
            generated
                .unix_mode()
                .expect("generated metadata permissions")
                & 0o777,
            0o644
        );
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
            .join(format!("cli-replay-seed-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(run_meta_dir(&root))
            .expect("run metadata directory should be created");
        let journal = engine::JournalWriter::create(
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
                    "tokens": { "input": 0, "output": 0, "cached_input": 0 },
                    "cost_microusd": 0,
                }),
            )
            .expect("LLM call should be recorded");

        let seed = replay_seed_from_journal(&root).expect("seed should be read");

        assert_eq!(seed, 12345);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn eval_case_summary_flags_flaky_cases() {
        let suite = EvalSuite {
            name: "smoke".into(),
            min_pass_rate: 1.0,
            repetitions: 2,
            cases: vec![
                EvalCase {
                    name: "stable".into(),
                    inputs: BTreeMap::new(),
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    seed: None,
                    assertions: vec![],
                },
                EvalCase {
                    name: "flaky".into(),
                    inputs: BTreeMap::new(),
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    seed: None,
                    assertions: vec![],
                },
            ],
        };
        let reports = vec![
            EvalCaseReport {
                name: "stable".into(),
                repetition: 0,
                passed: true,
                failures: vec![],
                tokens_total: 0,
                cost_microusd: 0,
                duration_ms: 0,
                llm_calls: 0,
            },
            EvalCaseReport {
                name: "stable".into(),
                repetition: 1,
                passed: true,
                failures: vec![],
                tokens_total: 0,
                cost_microusd: 0,
                duration_ms: 0,
                llm_calls: 0,
            },
            EvalCaseReport {
                name: "flaky".into(),
                repetition: 0,
                passed: true,
                failures: vec![],
                tokens_total: 0,
                cost_microusd: 0,
                duration_ms: 0,
                llm_calls: 0,
            },
            EvalCaseReport {
                name: "flaky".into(),
                repetition: 1,
                passed: false,
                failures: vec!["boom".into()],
                tokens_total: 0,
                cost_microusd: 0,
                duration_ms: 0,
                llm_calls: 0,
            },
        ];
        let (summary, flaky) = summarize_eval_cases(&suite, &reports);
        assert_eq!(summary.len(), 2);
        assert!(!summary[0].flaky);
        assert!(summary[1].flaky);
        assert_eq!(flaky, vec!["flaky".to_string()]);
        assert_eq!(percentile_u64(&[3, 1, 2]), 2);
        assert_eq!(percentile_u64(&[]), 0);
    }

    #[test]
    fn plan_command_allowlist_matches_exact_and_wildcard_args() {
        let manifest: contract::Manifest = toml::from_str(
            r#"
[generator]
id = "plan-fixture"
name = "Plan Fixture"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects = "none"
side_effects_scope = "invocation"

[permissions.containers]
enabled = false

[[permissions.commands]]
bin = "cc"
args = ["-o", "*"]
purpose = "compile"
isolation = "trusted_host""#,
        )
        .expect("fixture manifest should parse");
        assert!(plan_command_allowed(
            &manifest,
            &["cc".to_string(), "-o".to_string(), "hello".to_string()]
        ));
        assert!(!plan_command_allowed(
            &manifest,
            &["cc".to_string(), "-shared".to_string(), "hello".to_string()]
        ));
        assert!(!plan_command_allowed(&manifest, &["missing".to_string()]));
    }
}
