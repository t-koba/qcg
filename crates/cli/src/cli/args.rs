use camino::Utf8PathBuf;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Contract-driven harness for bounded, purpose-specialized generation",
    after_help = "Deployment scheduling env (strict): PREEMPTION=true|false (default true); PREEMPTION_MODE=step_boundary|interrupt (default step_boundary); PREEMPTION_QUEUE_POSITION=preserve|tail (default preserve). Run callers cannot override these policies.
Container daemon budgets (strict): CONTAINER_CREATE_TIMEOUT_SECS=600, CONTAINER_START_TIMEOUT_SECS=120, CONTAINER_ADMIN_TIMEOUT_SECS=60, CONTAINER_STOP_TIMEOUT_SECS=30; each accepts 1..=3600 seconds and does not limit workloads. See docs/operations.md."
)]
pub(crate) struct Cli {
    #[arg(long, global = true)]
    pub(crate) verbose: bool,
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    pub(crate) log_format: LogFormat,
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "PROVIDERS",
        help = "Path to the LLM, search, and MCP providers registry; defaults to ./providers.toml"
    )]
    pub(crate) providers: Option<Utf8PathBuf>,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Manage standalone ChatGPT subscription credentials (not inference).
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Create a minimal generator skeleton. Mechanism only: a valid qcg.toml.
    /// Policy content (prompts, schemas, budgets) stays with the author.
    New {
        path: Utf8PathBuf,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        force: bool,
    },
    Validate {
        path: Utf8PathBuf,
        #[arg(long)]
        json: bool,
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
        #[arg(long = "confirm", value_name = "ID=DECISION")]
        confirms: Vec<String>,
        #[arg(long = "confirmations-file")]
        confirmations_file: Option<Utf8PathBuf>,
        #[arg(long = "output", default_value = "out")]
        output: Utf8PathBuf,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        json: bool,
        /// Print the execution plan without running.
        #[arg(long)]
        plan: bool,
        /// Show read-only forecast diff with --plan (no execution, no writes).
        #[arg(long)]
        diff: bool,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-inputs-file-bytes", env = "MAX_INPUTS_FILE_BYTES")]
        max_inputs_file_bytes: Option<usize>,
    },
    Eval {
        generator: Utf8PathBuf,
        #[arg(long)]
        suite: Utf8PathBuf,
        #[arg(long = "output", default_value = ".data/evals")]
        output: Utf8PathBuf,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long, value_name = "REPORT_JSON")]
        baseline: Option<Utf8PathBuf>,
        #[arg(long)]
        json: bool,
    },
    List {
        #[arg(
            long = "generators-dir",
            env = "GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
    },
    /// List selectable LLM providers, models, and reasoning efforts.
    Models {
        /// Refresh external catalog sources before listing.
        #[arg(long)]
        refresh: bool,
        #[arg(long)]
        json: bool,
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
        /// Bounds entry count. Omitted means the 1000-entry default, never unlimited.
        #[arg(long = "max-entries", env = "PACKAGE_MAX_ENTRIES")]
        max_entries: Option<usize>,
        /// Bounds expanded bytes. Omitted means the 10 MiB default, never unlimited.
        #[arg(long = "max-bytes", env = "PACKAGE_MAX_BYTES")]
        max_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-metadata-bytes", env = "PACKAGE_MAX_METADATA_BYTES")]
        max_metadata_bytes: Option<usize>,
    },
    Install {
        source: String,
        #[arg(
            long = "generators-dir",
            env = "GENERATORS_DIR",
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
        /// Bounds entry count. Omitted means the 1000-entry default, never unlimited.
        #[arg(long = "max-entries", env = "PACKAGE_MAX_ENTRIES")]
        max_entries: Option<usize>,
        /// Bounds expanded bytes. Omitted means the 10 MiB default, never unlimited.
        #[arg(long = "max-bytes", env = "PACKAGE_MAX_BYTES")]
        max_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-archive-bytes", env = "PACKAGE_MAX_ARCHIVE_BYTES")]
        max_archive_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-metadata-bytes", env = "PACKAGE_MAX_METADATA_BYTES")]
        max_metadata_bytes: Option<usize>,
    },
    Uninstall {
        id: String,
        #[arg(
            long = "generators-dir",
            env = "GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long)]
        yes: bool,
    },
    Registry {
        #[command(subcommand)]
        command: RegistryCommand,
    },
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
    Search {
        query: String,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 0)]
        port: u16,
        #[arg(
            long = "generators-dir",
            env = "GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(
            long,
            env = "MAX_ACTIVE_RUNS",
            default_value_t = policy::DEFAULT_MAX_ACTIVE_RUNS
        )]
        max_active_runs: usize,
        #[arg(
            long,
            env = "MAX_TRACKED_RUNS",
            default_value_t = policy::DEFAULT_MAX_TRACKED_RUNS
        )]
        max_tracked_runs: usize,
        /// Deployment ceiling for per-run total steps. Omitted means the
        /// engine default; caps runs below their contract budget.
        #[arg(long = "max-total-steps", env = "MAX_TOTAL_STEPS")]
        max_total_steps: Option<usize>,
        #[arg(long, env = "RUN_STORE", value_enum, default_value_t = RunStoreArg::Exclusive)]
        run_store: RunStoreArg,
        #[arg(
            long = "cors-origin",
            env = "CORS_ORIGIN",
            value_delimiter = ',',
            value_name = "ORIGIN"
        )]
        cors_origins: Vec<String>,
        #[arg(long, env = "API_TOKEN", hide_env_values = true)]
        api_token: Option<String>,
        /// Explicit override. Omitted means the 2 MiB documented default, never unlimited.
        #[arg(long = "max-request-bytes", env = "MAX_REQUEST_BYTES")]
        max_request_bytes: Option<usize>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-artifact-bytes", env = "MAX_ARTIFACT_BYTES")]
        max_artifact_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-artifact-entries", env = "MAX_ARTIFACT_ENTRIES")]
        max_artifact_entries: Option<usize>,
        /// Omitted uses the 16 MiB hard asset default, never unlimited.
        #[arg(long = "max-asset-bytes", env = "MAX_ASSET_BYTES")]
        max_asset_bytes: Option<usize>,
    },
    /// Run a loopback server while polling the generators directory for
    /// changes; each change is reported on stderr.
    Dev {
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 0)]
        port: u16,
        #[arg(
            long = "generators-dir",
            env = "GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(
            long,
            env = "MAX_ACTIVE_RUNS",
            default_value_t = policy::DEFAULT_MAX_ACTIVE_RUNS
        )]
        max_active_runs: usize,
        /// Generator tree poll interval in milliseconds; must be greater
        /// than zero.
        #[arg(long = "watch-interval-ms", default_value_t = 500, value_parser = parse_watch_interval_ms)]
        watch_interval_ms: u64,
        /// After each change, run `eval` for this generator id; the
        /// suite is `<generators-dir>/<generator-id>/suite.json`.
        #[arg(long, value_name = "GENERATOR_ID")]
        eval: Option<String>,
    },
}

/// Reject a zero poll interval: a zero interval would spin the watch loop
/// without ever yielding, so it fails before any server work starts.
fn parse_watch_interval_ms(value: &str) -> Result<u64, String> {
    let millis = value.parse::<u64>().map_err(|error| {
        format!("watch interval must be a whole number of milliseconds: {error}")
    })?;
    if millis == 0 {
        return Err("watch interval must be greater than zero".to_string());
    }
    Ok(millis)
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RunStoreArg {
    Exclusive,
    #[value(name = "shared-filesystem")]
    SharedFilesystem,
}

impl From<RunStoreArg> for service::RunStoreMode {
    fn from(value: RunStoreArg) -> Self {
        match value {
            RunStoreArg::Exclusive => Self::Exclusive,
            RunStoreArg::SharedFilesystem => Self::SharedFilesystem,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum RegistryCommand {
    Add { name: String, url: String },
    Remove { name: String },
    List,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SkillCommand {
    /// Validate an Agent Skills directory (or a library of skills).
    Validate {
        path: Utf8PathBuf,
        /// Treat `path` as a directory of `<name>/SKILL.md` skills.
        #[arg(long)]
        library: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum RunsCommand {
    List {
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        generator: Option<String>,
    },
    Show {
        id: String,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
        /// Bundle failure causes from the journal into one screen.
        #[arg(long)]
        diagnose: bool,
    },
    Replay {
        id: String,
        generator: Option<Utf8PathBuf>,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long = "output")]
        output: Option<Utf8PathBuf>,
        #[arg(long = "reuse-seed")]
        reuse_seed: bool,
        #[arg(long = "answer")]
        answers: Vec<String>,
        #[arg(long = "confirm", value_name = "ID=DECISION")]
        confirms: Vec<String>,
        #[arg(long = "confirmations-file")]
        confirmations_file: Option<Utf8PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Fork {
        id: String,
        #[arg(long = "at-seq")]
        at_seq: u64,
        #[arg(long = "state-patch", value_name = "JSON_FILE")]
        state_patch: Option<Utf8PathBuf>,
        #[arg(long = "answer")]
        answers: Vec<String>,
        #[arg(long = "confirm", value_name = "ID=DECISION")]
        confirms: Vec<String>,
        #[arg(long = "confirmations-file")]
        confirmations_file: Option<Utf8PathBuf>,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
    },
    Trace {
        id: String,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long, value_name = "OTLP_JSON")]
        output: Option<Utf8PathBuf>,
        #[arg(long = "otlp-endpoint", value_name = "URL")]
        otlp_endpoint: Option<String>,
    },
    Costs {
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        generator: Option<String>,
        #[arg(long = "by-model")]
        by_model: bool,
        #[arg(long)]
        json: bool,
    },
    Gc {
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        /// Retain up to this many newest terminal runs; defaults to GC_KEEP.
        /// An explicit count overrides the deployment policy.
        #[arg(long)]
        keep: Option<usize>,
        /// Retain up to this many newest failed runs, separate from --keep;
        /// defaults to GC_KEEP_FAILED. A retention window recorded in a run's
        /// journal expires that run on its own: a run past its window is
        /// collected even when the count budget would keep it.
        #[arg(long = "keep-failed")]
        keep_failed: Option<usize>,
        #[arg(long)]
        delete: bool,
    },
    /// Irreversibly mark a durably successful run's outputs as invalid.
    Invalidate {
        id: String,
        #[arg(long)]
        reason: String,
        /// Descriptive audit label, not authenticated identity.
        #[arg(long)]
        actor_label: String,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
    },
    Delete {
        id: String,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
    },
    Export {
        id: String,
        #[arg(long = "runs-dir", default_value = ".data/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long = "output")]
        output: Option<Utf8PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum DocsCommand {
    StepSchemas,
    RunEvents,
    Openapi,
}

/// Account labels explicitly select isolated credential entries; there is no default account.
#[derive(Debug, Subcommand)]
pub(crate) enum AuthCommand {
    /// Sign in, or reauthenticate an existing account label.
    Login {
        #[arg(long)]
        account: String,
    },
    /// Inspect local expiry; optionally refresh when within 60 seconds of expiry.
    Status {
        #[arg(long)]
        account: String,
        #[arg(long)]
        refresh: bool,
    },
    /// Clear local credentials and attempt remote revocation unless --local.
    Logout {
        #[arg(long)]
        account: String,
        #[arg(long)]
        local: bool,
        /// Also delete the saved client/account registration mapping.
        #[arg(long)]
        forget: bool,
    },
}
