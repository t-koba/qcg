use camino::Utf8PathBuf;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "qcg",
    version,
    about = "Contract-driven harness for bounded, purpose-specialized generation"
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
        env = "QCG_PROVIDERS",
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
        #[arg(long = "max-inputs-file-bytes", env = "QCG_MAX_INPUTS_FILE_BYTES")]
        max_inputs_file_bytes: Option<usize>,
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
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-entries", env = "QCG_PACKAGE_MAX_ENTRIES")]
        max_entries: Option<usize>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-bytes", env = "QCG_PACKAGE_MAX_BYTES")]
        max_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-metadata-bytes", env = "QCG_PACKAGE_MAX_METADATA_BYTES")]
        max_metadata_bytes: Option<usize>,
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
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-entries", env = "QCG_PACKAGE_MAX_ENTRIES")]
        max_entries: Option<usize>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-bytes", env = "QCG_PACKAGE_MAX_BYTES")]
        max_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-archive-bytes", env = "QCG_PACKAGE_MAX_ARCHIVE_BYTES")]
        max_archive_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-metadata-bytes", env = "QCG_PACKAGE_MAX_METADATA_BYTES")]
        max_metadata_bytes: Option<usize>,
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
    Registry {
        #[command(subcommand)]
        command: RegistryCommand,
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
            env = "QCG_GENERATORS_DIR",
            default_value = "generators"
        )]
        generators_dir: Utf8PathBuf,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(
            long,
            env = "QCG_MAX_ACTIVE_RUNS",
            default_value_t = qcg_policy::DEFAULT_MAX_ACTIVE_RUNS
        )]
        max_active_runs: usize,
        #[arg(
            long,
            env = "QCG_MAX_TRACKED_RUNS",
            default_value_t = qcg_policy::DEFAULT_MAX_TRACKED_RUNS
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
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-request-bytes", env = "QCG_MAX_REQUEST_BYTES")]
        max_request_bytes: Option<usize>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-artifact-bytes", env = "QCG_MAX_ARTIFACT_BYTES")]
        max_artifact_bytes: Option<u64>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-artifact-entries", env = "QCG_MAX_ARTIFACT_ENTRIES")]
        max_artifact_entries: Option<usize>,
        /// Explicit max only. Omitted means no mechanistic limit.
        #[arg(long = "max-asset-bytes", env = "QCG_MAX_ASSET_BYTES")]
        max_asset_bytes: Option<usize>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RunStoreArg {
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
pub(crate) enum RegistryCommand {
    Add { name: String, url: String },
    Remove { name: String },
    List,
}

#[derive(Debug, Subcommand)]
pub(crate) enum RunsCommand {
    List {
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        generator: Option<String>,
    },
    Show {
        id: String,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
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
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
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
    Costs {
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
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
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long, default_value_t = 50)]
        keep: usize,
        /// Retain at least this many failed runs even when they fall outside --keep.
        #[arg(long = "keep-failed", default_value_t = 10)]
        keep_failed: usize,
        #[arg(long)]
        delete: bool,
    },
    Delete {
        id: String,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
        runs_dir: Utf8PathBuf,
        #[arg(long)]
        json: bool,
    },
    Export {
        id: String,
        #[arg(long = "runs-dir", default_value = ".qcg/runs")]
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
