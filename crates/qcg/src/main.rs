use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use clap::Parser;
use qcg_api::{ForkRun, ForkStatePatch, RunStatus};
use qcg_contract::Contract;
use qcg_service::{DirectRun, LocalQcgService, step_param_schemas_markdown};
use serde_json::json;
use std::collections::BTreeSet;

mod registry;

mod cli;

use cli::args::{Cli, Command, DocsCommand, RegistryCommand, RunsCommand, SkillCommand};
use cli::eval::run_eval;
use cli::gc::{auto_gc_runs, gc_runs};
use cli::inputs::{load_answers, load_confirmations, load_inputs};
use cli::install::{InstallVerification, install, sign_package, uninstall};
use cli::package_cmd::{package, sha256_file};
use cli::plan::print_run_plan;
use cli::replay::{ReplayRequest, export_run_trace, replay_run};
use cli::runs_cli::{list_runs, show_costs, show_run};
use cli::setup::{bundled_generators_root, init_tracing};
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;
use qcg_service::app_registry_with_providers as app_registry;

// Windows main threads default to a 1 MiB stack, which overflows while clap
// builds the CLI command tree. Run everything on a thread with an explicit
// 8 MiB stack so behaviour is identical on every platform.
const MAIN_THREAD_STACK_BYTES: usize = 8 * 1024 * 1024;

/// CLI-owned service construction (E04): every CLI command builds the
/// service through the policy constructor with explicit CLI defaults.
/// No production path may use the test-only `LocalQcgService::new`.
pub(crate) fn local_cli_service(
    generators_dir: Utf8PathBuf,
    runs_dir: Utf8PathBuf,
    providers_path: Option<Utf8PathBuf>,
) -> Result<LocalQcgService> {
    Ok(LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        providers_path,
        qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
        qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
        qcg_service::RunStoreMode::Exclusive,
        qcg_service::ServiceDeploymentPolicy::default(),
    )?)
}

/// Shared serve boot for `qcg serve` and `qcg dev`. Resolve once before
/// binding (E04 single freeze): validation and serving observe identical
/// values; a refused boot never occupies the port.
pub(crate) async fn serve_with_config(
    config: qcg_server::ServerConfig,
    bind: &str,
    port: u16,
) -> Result<()> {
    let policy = qcg_server::resolve_server_policy(&config)
        .map_err(|detail| anyhow::anyhow!("invalid server configuration: {detail}"))?;
    let addr: std::net::SocketAddr = format!("{bind}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;
    println!("qcg server listening on http://{actual_addr}");
    qcg_server::serve_with_resolved_policy(policy, config, listener).await?;
    Ok(())
}

fn main() -> Result<std::process::ExitCode> {
    std::thread::Builder::new()
        .name("qcg-main".to_owned())
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
        Command::Validate { path, json } => {
            if !json {
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
            let runs_dir = Utf8PathBuf::from(".qcg/runs");
            let service = local_cli_service(Utf8PathBuf::new(), runs_dir.clone(), providers_path)?;
            auto_gc_runs(&runs_dir)?;
            let run = DirectRun {
                generator_path: generator,
                inputs,
                output_dir: output,
                json_events: false,
                interactive: !yes,
                answers,
                confirmations,
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
                    if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
                        anyhow::bail!(
                            "generator roots contain more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
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
        Command::Models { refresh, json } => {
            let service = local_cli_service(
                Utf8PathBuf::new(),
                Utf8PathBuf::from(".qcg/runs"),
                providers_path.clone(),
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
                replay_run(ReplayRequest {
                    runs_dir: runs_dir.clone(),
                    id: id.clone(),
                    generator: generator.clone(),
                    output: output.clone(),
                    reuse_seed,
                    answers: load_answers(answers)?,
                    confirmations: load_confirmations(confirms, confirmations_file)?,
                    json_output: json,
                    providers_path: providers_path.clone(),
                })
                .await?
            }
            RunsCommand::Fork {
                id,
                at_seq,
                state_patch,
                answers,
                confirms,
                confirmations_file,
                runs_dir,
                json,
            } => {
                let state_patch = match state_patch {
                    Some(path) => serde_json::from_slice::<ForkStatePatch>(&qcg_fs::read_bounded(
                        &path, None,
                    )?)
                    .with_context(|| format!("failed to parse state patch `{path}`"))?,
                    None => ForkStatePatch::default(),
                };
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, providers_path)?;
                let fork_id = service
                    .fork_run(
                        &id,
                        ForkRun {
                            at_seq,
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
                keep_failed,
                delete,
            } => gc_runs(&runs_dir, keep, keep_failed, delete)?,
            RunsCommand::Delete { id, runs_dir, json } => {
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, providers_path)?;
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
                let service = local_cli_service(Utf8PathBuf::new(), runs_dir, providers_path)?;
                let parts = service.run_bundle_parts(&id).await?;
                let output =
                    output.unwrap_or_else(|| Utf8PathBuf::from(format!("{id}-bundle.zip")));
                let file = std::fs::File::create(&output)
                    .with_context(|| format!("failed to create bundle `{output}`"))?;
                qcg_service::write_run_bundle_stream_with_limits(
                    &parts.snapshot,
                    &parts.inputs,
                    &parts.journal,
                    parts.outputs.as_ref(),
                    &parts.verified,
                    file,
                    &qcg_service::ArtifactZipLimits::default(),
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
            let output = output.unwrap_or_else(|| {
                Utf8PathBuf::from(format!("{}.qcg", dir.file_name().unwrap_or("generator")))
            });
            let limits = qcg_service::PackageLimits {
                max_entries,
                max_bytes,
                max_metadata_bytes,
                max_archive_bytes: None,
            };
            package(&dir, &output, &limits)?;
            println!("sha256 {}", sha256_file(&output, limits.max_archive_bytes)?);
            if let Some(signing_key) = signing_key {
                let bytes = qcg_fs::read_bounded(
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
                &qcg_service::PackageLimits {
                    max_entries,
                    max_bytes,
                    max_metadata_bytes,
                    max_archive_bytes,
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
            max_artifact_entries,
            max_asset_bytes,
        } => {
            let api_token = resolve_api_token(api_token)?;
            let config = qcg_server::ServerConfig {
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
                max_artifact_entries,
                max_asset_bytes,
            };
            serve_with_config(config, &bind, port).await?;
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
            let config = qcg_server::ServerConfig {
                providers_path,
                extra_generators_dirs: bundled_generators_root()
                    .filter(|bundled| bundled != &generators_dir)
                    .into_iter()
                    .collect(),
                generators_dir,
                runs_dir,
                max_active_runs,
                max_tracked_runs: qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
                max_total_steps: None,
                run_store_mode: qcg_service::RunStoreMode::Exclusive,
                cors_origins: Vec::new(),
                api_token: None,
                max_request_bytes: None,
                max_artifact_bytes: None,
                max_artifact_entries: None,
                max_asset_bytes: None,
            };
            cli::dev::run_dev(config, bind, port, watch_interval_ms, eval).await?;
        }
    }
    Ok(std::process::ExitCode::SUCCESS)
}

/// Resolves the instance bearer token: an explicit flag or `QCG_API_TOKEN`
/// wins, then `QCG_API_TOKEN_FILE` (the only documented file source). An
/// unreadable or empty file fails closed instead of booting unauthenticated.
fn resolve_api_token(explicit: Option<String>) -> anyhow::Result<Option<String>> {
    if explicit.is_some() {
        return Ok(explicit);
    }
    let Some(path) = std::env::var_os("QCG_API_TOKEN_FILE") else {
        return Ok(None);
    };
    let path =
        camino::Utf8PathBuf::from_path_buf(std::path::PathBuf::from(path)).map_err(|path| {
            anyhow::anyhow!("QCG_API_TOKEN_FILE is not valid UTF-8: {}", path.display())
        })?;
    let bytes = qcg_fs::read_bounded(&path, Some(qcg_policy::MAX_CREDENTIAL_FILE_BYTES as usize))
        .map_err(|error| {
        anyhow::anyhow!("failed to read QCG_API_TOKEN_FILE `{path}`: {error}")
    })?;
    let token = String::from_utf8(bytes)
        .map_err(|error| anyhow::anyhow!("QCG_API_TOKEN_FILE `{path}` is not UTF-8: {error}"))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("QCG_API_TOKEN_FILE `{path}` is empty");
    }
    Ok(Some(token))
}

fn print_llm_catalog(catalog: &qcg_api::LlmCatalogResponse) {
    println!("provider\tmodel\tenabled\tavailable\tefforts\tinput\toutput\tcontext\tsource");
    let price = |value: Option<f64>| {
        value
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    };
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
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                provider.id,
                model.id,
                model.enabled,
                provider.available,
                efforts,
                price(model.input_cost_per_million_usd),
                price(model.output_cost_per_million_usd),
                context,
                model.source
            );
        }
        if let Some(error) = &provider.error {
            eprintln!("provider {}: {error}", provider.id);
        }
    }
    if catalog.stale {
        eprintln!("catalog metadata is stale; run `qcg models --refresh`");
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
    use crate::cli::gc::gc_runs_impl;
    use crate::cli::install::{CleanupPaths, commit_install, verify_package_bytes};
    use crate::cli::plan::{plan_command_allowed, read_bounded_confirmation};
    use crate::cli::replay::replay_seed_from_journal;
    use crate::cli::runs_cli::{find_pricing, format_usd};
    use aws_lc_rs::rand::SystemRandom;
    use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
    use qcg_policy::MAX_CONFIRM_INPUT_BYTES;
    use qcg_service::run_meta_dir;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::fs::File;
    use uuid::Uuid;

    #[test]
    fn usd_formatting_and_pricing_lookup_cover_display_cases() {
        assert_eq!(format_usd(0), "$0.000000");
        assert_eq!(format_usd(1), "$0.000001");
        assert_eq!(format_usd(4231), "$0.004231");
        assert_eq!(format_usd(1_000_000), "$1.000000");

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("qcg-pricing-test-{}", Uuid::now_v7()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("package directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "pricing-fixture"
name = "Pricing Fixture"
version = "0.1.0"
qcg_version = "^0.1"

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
        assert_eq!(find_pricing(&contract, "main", "large"), Some((2.5, 10.0)));
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
    fn silent_gc_honors_retention_days_without_reporting() {
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

[retention]
days = 0

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
                    "retention_days": 0,
                }),
            )
            .expect("run should start");
        journal
            .event("run_finished", json!({ "status": "success" }))
            .expect("run should finish");

        gc_runs_impl(&runs_dir, 50, 10, true, false).expect("silent GC should run");

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
        let limits = qcg_service::PackageLimits::default();
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-cli-copy-test-{}", Uuid::now_v7()));
        let source = root.join("source");
        let target = root.join("target");
        std::fs::create_dir_all(&source).expect("source directory should be created");
        std::fs::write(source.join("file.txt"), "copy me").expect("source file should be written");
        assert!(
            qcg_service::package::copy_dir_all(&root.join("missing"), &target, &limits).is_err()
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(source.join("file.txt"), source.join("link.txt"))
                .expect("symbolic link should be created");
            assert!(qcg_service::package::copy_dir_all(&source, &target, &limits).is_err());
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

        package(&source, &output, &qcg_service::PackageLimits::default())
            .expect("package should be written");

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
        let manifest: qcg_contract::Manifest = toml::from_str(
            r#"
[generator]
id = "plan-fixture"
name = "Plan Fixture"
version = "0.1.0"
qcg_version = "^0.1"

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
