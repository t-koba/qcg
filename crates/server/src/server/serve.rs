use anyhow::Result;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderValue;
use axum::http::Method;
#[cfg(feature = "server-cors")]
use axum::http::header;
use axum::middleware as axum_middleware;
use axum::routing::{get, put};
use service::LocalService;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "server-cors")]
use tower_http::cors::CorsLayer;

use super::config::{AppState, ServerConfig};
use super::generators::{
    describe_generator, healthz, list_generators, llm_catalog, openapi, read_generator_asset,
};
use super::mcp::{
    cancel_pending_mcp_authorization, clear_mcp_authorization, complete_mcp_authorization,
    list_mcp_servers, start_mcp_authorization,
};
use super::middleware::{
    loopback_oauth_origins, metrics, reject_unsafe_generator_asset_path, require_api_auth,
    security_headers_middleware, sha256_bytes,
};
use super::rate_limit::{RateLimitOverflow, RateLimitPolicy, RateLimiter, enforce_rate_limit};
use super::run_detail::{
    answer_run, cancel_run_from_path, confirm_run, delete_run, dispatch_fallback, read_artifact,
    read_artifacts_zip, read_cost_metrics, read_journal, read_run_bundle, run_artifacts,
    run_events, run_snapshot,
};
use super::runs::{fork_run, list_runs, start_run};
#[cfg(feature = "server-cors")]
use policy::IDEMPOTENCY_HEADER;

pub async fn serve_with_listener(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
) -> Result<SocketAddr> {
    // Production resolves once here so the environment drives both phases;
    // tests keep their explicit short-deadline override below.
    let policy = resolve_server_policy(&config).map_err(|detail| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid server configuration: {detail}"),
        )
    })?;
    let settle = policy.shutdown_settle;
    serve_with_resolved_policy_and_deadline(policy, config, listener, settle).await
}

/// Serve with an already-resolved policy: the single-freeze boot path.
/// `main.rs` resolves once before bind and passes the frozen policy here,
/// so validation and use observe identical values even if the environment
/// changes mid-boot (E04). Direct callers may keep using
/// `serve_with_listener` (resolve + delegate, one internal resolution).
pub async fn serve_with_resolved_policy(
    policy: ResolvedServerPolicy,
    config: ServerConfig,
    listener: tokio::net::TcpListener,
) -> Result<SocketAddr> {
    let settle = policy.shutdown_settle;
    serve_with_resolved_policy_and_deadline(policy, config, listener, settle).await
}

/// Serve with an explicit outer shutdown deadline. Production uses
/// [`SHUTDOWN_DEADLINE`]; tests pass a short deadline so the timeout path
/// surfaces fast instead of waiting 150 s (E05).
/// Validates deployment policy without binding, locking, or creating
/// directories: callers must run this before `TcpListener::bind` so a
/// refused boot leaves no occupied port behind (E04). `serve_*` re-runs
/// the same check after bind as defense in depth for direct callers that
/// skip pre-validation; the pre-bind call is what keeps the port free.
/// Port-free refusal holds only on that path (E04): `serve_with_listener`
/// takes an already-bound listener, so a post-bind refusal still holds
/// the port — pre-validate before bind when port occupation matters.
/// Service construction (lock + directories) happens before router build,
/// but no resident task or recovery runs until the router succeeds, so a
/// router failure drops the service with its lock released and zero
/// recovered side effects (E04).
/// Deployment policy resolved once per boot from flags and the environment.
/// Resolving once (instead of validating strings first and re-reading the
/// environment later) keeps validation and use on the same values even if
/// the environment changes mid-boot (E04). Every `ServerConfig` field is
/// validated here with explicit fail-closed bounds: a refused boot leaves
/// no occupied port, no lock, and no recovery behind (E04).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedServerPolicy {
    pub read_cache_max_bytes: usize,
    pub idempotency_ttl: std::time::Duration,
    pub idempotency_max_entries: usize,
    pub max_total_steps: Option<usize>,
    pub validated_cors: Vec<HeaderValue>,
    pub auto_gc: bool,
    pub preemption_enabled: bool,
    /// Deployment HTTP rate limit resolved at boot (E04 freeze). `None`
    /// leaves the server unthrottled; `Some` installs one shared
    /// anonymous/instance bucket plus one bucket for verified identity
    /// (F10), never one bucket per unverified presented string.
    pub rate_limit: Option<RateLimitPolicy>,
    /// Deployment audit floor resolved at boot (E04 freeze).
    pub audit_floor: policy::AuditFloor,
    /// Resident OTLP export resolved at boot (E04 freeze). `None` disables
    /// export entirely.
    pub otlp: Option<super::otlp::OtlpConfig>,
    /// Retention sweep policy resolved at boot (E04 freeze).
    pub gc_keep: usize,
    pub gc_keep_failed: usize,
    pub gc_interval_secs: u64,
    /// Deployment cap on parallel wave scheduling; `None` uses the CPU count.
    pub max_parallel_steps: Option<usize>,
    /// Metrics cardinality policy resolved at boot (E04 freeze).
    pub metrics_policy: super::middleware::MetricsPolicy,
    /// Per-run live broadcast channel capacity.
    pub live_event_channel_capacity: usize,
    /// Shared-mode journal follow cadence, in milliseconds.
    pub journal_poll_interval_millis: u64,
    /// Directory scan cap for run and generator roots.
    pub max_directory_scan_entries: usize,
    /// Shared-store abandoned-run rescan cadence, in seconds.
    pub shared_store_rescan_secs: u64,
    /// Queued-run resumer cadence, in seconds.
    pub queued_resumer_secs: u64,
    /// Graceful HTTP drain timeout before settlement proceeds.
    pub shutdown_drain: std::time::Duration,
    /// Outer graceful settlement deadline after the drain.
    pub shutdown_settle: std::time::Duration,
}

fn reject_zero_explicit_max(config: &ServerConfig) -> Result<(), String> {
    if config.max_request_bytes == Some(0) {
        return Err("max_request_bytes must be greater than zero when set".into());
    }
    if config.max_artifact_bytes == Some(0) {
        return Err("max_artifact_bytes must be greater than zero when set".into());
    }
    if config.max_artifact_entries == Some(0) {
        return Err("max_artifact_entries must be greater than zero when set".into());
    }
    if config.max_asset_bytes == Some(0) {
        return Err("max_asset_bytes must be greater than zero when set".into());
    }
    Ok(())
}

pub fn resolve_server_policy(config: &ServerConfig) -> Result<ResolvedServerPolicy, String> {
    let idempotency_ttl = super::idempotency::effective_idempotency_ttl()
        .map_err(|detail| format!("invalid idempotency configuration: {detail}"))?;
    let idempotency_max_entries = super::idempotency::effective_idempotency_max_entries()
        .map_err(|detail| format!("invalid idempotency configuration: {detail}"))?;
    let max_total_steps = super::config::effective_max_total_steps(config.max_total_steps)
        .map_err(|detail| format!("invalid max_total_steps step budget configuration: {detail}"))?;
    let validated_cors = parse_cors_origins(&config.cors_origins)
        .map_err(|detail| format!("invalid CORS configuration: {detail}"))?;
    let auto_gc = policy::parse_bool_env("AUTO_GC", true)
        .map_err(|detail| format!("invalid GC configuration: {detail}"))?;
    let preemption_enabled = policy::parse_bool_env("PREEMPTION", true)
        .map_err(|detail| format!("invalid preemption configuration: {detail}"))?;
    // Rate limit: strict positive integers only, never a silent fold. Unset
    // rps disables the layer; every other invalid or incomplete combination
    // refuses boot (E04).
    let rate_limit = super::rate_limit::resolve_rate_limit_policy()
        .map_err(|detail| format!("invalid rate limit configuration: {detail}"))?;
    let metrics_policy = super::middleware::resolve_metrics_policy()
        .map_err(|detail| format!("invalid metrics configuration: {detail}"))?;
    let otlp = super::otlp::resolve_otlp_config()?;
    // Retention sweep policy: strict positive integers, resolved once.
    let live_event_channel_capacity = usize::try_from(parse_bounded_env(
        "LIVE_EVENT_CHANNEL_CAPACITY",
        policy::LIVE_EVENT_CHANNEL_CAPACITY as u64,
        policy::MIN_LIVE_EVENT_CHANNEL_CAPACITY as u64,
        policy::MAX_LIVE_EVENT_CHANNEL_CAPACITY as u64,
    )?)
    .map_err(|_| "LIVE_EVENT_CHANNEL_CAPACITY is not representable".to_string())?;
    let journal_poll_interval_millis = parse_bounded_env(
        "JOURNAL_POLL_INTERVAL_MS",
        policy::JOURNAL_POLL_INTERVAL_MILLIS,
        policy::MIN_JOURNAL_POLL_INTERVAL_MILLIS,
        policy::MAX_JOURNAL_POLL_INTERVAL_MILLIS,
    )?;
    let max_directory_scan_entries = usize::try_from(parse_bounded_env(
        "MAX_DIRECTORY_SCAN_ENTRIES",
        policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES as u64,
        policy::MIN_DIRECTORY_SCAN_ENTRIES as u64,
        policy::MAX_DIRECTORY_SCAN_ENTRIES as u64,
    )?)
    .map_err(|_| "MAX_DIRECTORY_SCAN_ENTRIES is not representable".to_string())?;
    let shared_store_rescan_secs = parse_bounded_env(
        "SHARED_RESCAN_SECS",
        5,
        policy::MIN_STORE_RESCAN_SECS,
        policy::MAX_STORE_RESCAN_SECS,
    )?;
    let queued_resumer_secs = parse_bounded_env(
        "QUEUED_RESUMER_SECS",
        5,
        policy::MIN_STORE_RESCAN_SECS,
        policy::MAX_STORE_RESCAN_SECS,
    )?;
    let shutdown_drain = std::time::Duration::from_secs(parse_bounded_env(
        "SHUTDOWN_DRAIN_SECS",
        30,
        policy::MIN_SHUTDOWN_PHASE_SECS,
        policy::MAX_SHUTDOWN_PHASE_SECS,
    )?);
    let shutdown_settle = std::time::Duration::from_secs(parse_bounded_env(
        "SHUTDOWN_SETTLE_SECS",
        150,
        policy::MIN_SHUTDOWN_PHASE_SECS,
        policy::MAX_SHUTDOWN_PHASE_SECS,
    )?);
    if shutdown_settle < shutdown_drain {
        return Err("SHUTDOWN_SETTLE_SECS must be at least SHUTDOWN_DRAIN_SECS".to_string());
    }
    let max_parallel_steps = match std::env::var("MAX_PARALLEL_STEPS") {
        Err(_) => None,
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed >= 1 => Some(parsed),
            _ => {
                return Err(format!(
                    "invalid MAX_PARALLEL_STEPS `{value}`: must be an integer of at least 1"
                ));
            }
        },
    };
    let read_cache_max_bytes =
        parse_positive_env("READ_CACHE_MAX_BYTES", policy::DEFAULT_READ_CACHE_BYTES)?;
    let gc_keep = parse_positive_env("GC_KEEP", policy::DEFAULT_GC_KEEP)?;
    let gc_keep_failed = parse_positive_env("GC_KEEP_FAILED", policy::DEFAULT_GC_KEEP_FAILED)?;
    let gc_interval_secs = match std::env::var("GC_INTERVAL_SECS") {
        Err(_) => policy::DEFAULT_GC_INTERVAL_SECS,
        Ok(value) => match value.parse::<u64>() {
            Ok(seconds) if seconds >= 60 => seconds,
            _ => {
                return Err(format!(
                    "invalid GC_INTERVAL_SECS `{value}`: must be an integer of at least 60"
                ));
            }
        },
    };
    // Audit floor: strict values only, never a silent truthy/falsy fold.
    let audit_floor = match std::env::var("AUDIT_FLOOR") {
        Err(_) => policy::AuditFloor::Minimal,
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "minimal" => policy::AuditFloor::Minimal,
            "standard" => policy::AuditFloor::Standard,
            _ => {
                return Err(format!(
                    "invalid AUDIT_FLOOR `{value}`: must be `minimal` or `standard`"
                ));
            }
        },
    };
    // Run-slot bounds: process-local capacity, not access control (E04).
    // Upper bounds fail closed so an absurd flag cannot OOM the tracker.
    // Rationale: max_active_runs caps live engine tasks plus their permits
    // and channels (one 512-slot broadcast plus one task slot per run);
    // 65536 bounds that residency under ~tens of GB worst case instead of
    // unbounded growth. max_tracked_runs caps retained RunRecords (memory
    // map plus rehydrated state); 1000000 bounds the map itself. Both are
    // OOM guards, not tuned capacity targets: defaults stay 8 / 4096
    // (`policy::limits`). Retuning either bound must re-justify the
    // residency math here.
    if config.max_active_runs == 0 {
        return Err("max_active_runs must be greater than zero".into());
    }
    if config.max_active_runs > 65_536 {
        return Err("max_active_runs must be at most 65536".into());
    }
    if config.max_tracked_runs < config.max_active_runs {
        return Err(format!(
            "max_tracked_runs must be at least max_active_runs ({})",
            config.max_active_runs
        ));
    }
    if config.max_tracked_runs == 0 {
        return Err("max_tracked_runs must be greater than zero".into());
    }
    if config.max_tracked_runs > 1_000_000 {
        return Err("max_tracked_runs must be at most 1000000".into());
    }
    // Explicit-max-only byte/count knobs: None means no mechanistic limit;
    // Some(0) would block every request, so it refuses boot (E04).
    reject_zero_explicit_max(config)?;
    // Run-store mode is an exhaustive enum: every variant is explicitly
    // accepted here so adding a variant forces a boot decision (E04).
    match config.run_store_mode {
        service::RunStoreMode::Exclusive | service::RunStoreMode::SharedFilesystem => {}
    }
    // Directory knobs: non-empty paths only. Existence and lockability are
    // decided at service construction (still before router/resume/resident
    // start, with zero recovery on refusal); an empty path would silently
    // resolve to the process cwd, so it refuses here (E04).
    if config.generators_dir.as_str().is_empty() {
        return Err("generators_dir must not be empty".into());
    }
    if config.runs_dir.as_str().is_empty() {
        return Err("runs_dir must not be empty".into());
    }
    for extra in &config.extra_generators_dirs {
        if extra.as_str().is_empty() {
            return Err("extra_generators_dirs must not contain an empty path".into());
        }
    }
    // Providers path: an empty explicit path would silently fall back to
    // another registry, so it refuses here. A missing file still fails at
    // service construction (before router/resume/resident start) with zero
    // recovery (E04).
    if let Some(providers_path) = &config.providers_path
        && providers_path.as_str().is_empty()
    {
        return Err("providers_path must not be empty when set".into());
    }
    // Bearer token: an empty explicit token would never authenticate yet
    // looks configured, so it refuses here. The digest is computed from the
    // exact configured value after this check (E04).
    if let Some(token) = &config.api_token
        && token.is_empty()
    {
        return Err("api_token must not be empty when set".into());
    }
    Ok(ResolvedServerPolicy {
        metrics_policy,
        read_cache_max_bytes,
        idempotency_ttl,
        idempotency_max_entries,
        max_total_steps,
        validated_cors,
        auto_gc,
        preemption_enabled,
        rate_limit,
        audit_floor,
        otlp,
        gc_keep,
        gc_keep_failed,
        gc_interval_secs,
        max_parallel_steps,
        live_event_channel_capacity,
        journal_poll_interval_millis,
        max_directory_scan_entries,
        shared_store_rescan_secs,
        queued_resumer_secs,
        shutdown_drain,
        shutdown_settle,
    })
}

/// Strict bounded-integer env parse: out-of-range and unparseable values
/// refuse boot with the variable name, its bounds, and the actual value.
fn parse_bounded_env(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(value) => match value.parse::<u64>() {
            Ok(parsed) if (min..=max).contains(&parsed) => Ok(parsed),
            _ => Err(format!(
                "invalid {name} `{value}`: must be an integer between {min} and {max}"
            )),
        },
    }
}

/// Strict positive-integer env parse: invalid, zero, and negative values
/// refuse boot with an error naming the variable (E04).
fn parse_positive_env(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed >= 1 => Ok(parsed),
            _ => Err(format!(
                "invalid {name} `{value}`: must be an integer of at least 1"
            )),
        },
    }
}

pub(crate) async fn serve_with_resolved_policy_and_deadline(
    policy: ResolvedServerPolicy,
    config: ServerConfig,
    listener: tokio::net::TcpListener,
    shutdown_deadline: std::time::Duration,
) -> Result<SocketAddr> {
    // The listener is already bound by the caller: local_addr is a pure
    // query with no new port occupation. The main path resolved before bind
    // (keeping refused boots from occupying the port); this resolved path
    // never re-reads the environment, so the frozen policy is authoritative
    // even if the environment changed after bind (E04 single freeze).
    // Total shutdown bound stays linked to its two phases (E05).
    debug_assert_eq!(TOTAL_SHUTDOWN_BOUND, DRAIN_TIMEOUT + SHUTDOWN_DEADLINE);
    let actual_addr = listener.local_addr()?;
    // The frozen policy was validated before the service is created: a
    // misconfigured boot fails before acquiring the run-store lock or
    // creating directories, with zero recovered side effects and no
    // resident task left behind (E04). Host-resolved operational policy
    // lives here, not inside the service: automatic GC, priority
    // preemption, and idempotency retention are startup choices with
    // explicit environment overrides (3.2). Invalid knobs refuse to boot
    // instead of degrading silently.
    let ResolvedServerPolicy {
        metrics_policy,
        read_cache_max_bytes,
        idempotency_ttl,
        idempotency_max_entries,
        max_total_steps,
        validated_cors,
        auto_gc,
        preemption_enabled,
        rate_limit,
        audit_floor,
        otlp,
        gc_keep,
        gc_keep_failed,
        gc_interval_secs,
        max_parallel_steps,
        live_event_channel_capacity,
        journal_poll_interval_millis,
        max_directory_scan_entries,
        shared_store_rescan_secs,
        queued_resumer_secs,
        shutdown_drain,
        shutdown_settle: _,
    } = policy;
    tracing::info!(
        idempotency_ttl_secs = idempotency_ttl.as_secs(),
        idempotency_max_entries,
        max_request_bytes = config.max_request_bytes,
        max_total_steps = max_total_steps,
        auto_gc,
        preemption_enabled,
        rate_limit_rps = rate_limit.as_ref().map(|policy| policy.rps),
        rate_limit_burst = rate_limit.as_ref().map(|policy| policy.burst),
        rate_limit_max_identities = rate_limit.as_ref().map(|policy| policy.max_identities),
        rate_limit_overflow = rate_limit
            .as_ref()
            .map(|policy| match policy.overflow {
                RateLimitOverflow::FoldAnonymous => "fold",
                RateLimitOverflow::RejectNew => "reject",
            })
            .unwrap_or("off"),
        metrics_generator_limit = metrics_policy.generator_limit,
        metrics_pinned_generators = metrics_policy.pinned_generators.len(),
        validated_cors_origins = validated_cors.len(),
        otlp = otlp
            .as_ref()
            .map(|config| config.redacted_endpoint())
            .unwrap_or_default(),
        "effective server policy",
    );
    let mut roots = vec![config.generators_dir.clone()];
    roots.extend(config.extra_generators_dirs.clone());
    // Deployment policy enters through the constructor, never through
    // post-construction setters that could race recovery execution (E04).
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        roots,
        config.runs_dir.clone(),
        config.providers_path.clone(),
        config.max_active_runs,
        config.max_tracked_runs,
        config.run_store_mode,
        service::ServiceDeploymentPolicy {
            read_cache_max_bytes,
            max_total_steps,
            preemption_enabled,
            audit_floor,
            gc_keep,
            gc_keep_failed,
            gc_interval_secs,
            max_parallel_steps,
            live_event_channel_capacity,
            journal_poll_interval_millis,
            max_directory_scan_entries,
            shared_store_rescan_secs,
            queued_resumer_secs,
        },
    )?;
    let oauth_origin = actual_addr
        .ip()
        .is_loopback()
        .then(|| format!("http://{actual_addr}"));
    let oauth_allowed_origins = loopback_oauth_origins(actual_addr);
    let oauth_callback_url = oauth_origin
        .as_ref()
        .map(|origin| format!("{origin}/api/mcp/oauth/callback"));
    // One shutdown token for both the server surface and the service
    // itself: an embedded service alone must still be stoppable, and a
    // cancelled token must reach resident tasks, SSE, and admissions
    // without a second source of truth (E05).
    let shutdown = service.shutdown_token();
    let state = Arc::new(AppState {
        service,
        runs_dir: config.runs_dir.clone(),
        oauth_origin,
        oauth_allowed_origins,
        oauth_callback_url,
        idempotency: tokio::sync::Mutex::new(BTreeMap::new()),
        // Freeze the already-validated boot policy: requests never re-read
        // the environment (E04).
        idempotency_ttl,
        idempotency_max_entries,
        api_token_digest: config.api_token.as_deref().map(sha256_bytes),
        artifact_limits: service::ArtifactZipLimits {
            max_bytes: config.max_artifact_bytes,
            max_entries: config.max_artifact_entries,
        },
        asset_limit: config.max_asset_bytes,
        max_request_bytes: config.max_request_bytes,
        // Freeze the already-resolved metrics policy: scrapes never
        // re-read the environment (E04).
        metrics_policy,
        shutdown: shutdown.clone(),
    });
    // Router construction is the last fallible initialization step. It must
    // complete before recovery or resident tasks start so a refused boot
    // has no recovered side effects (E04). The validated CORS values from
    // above flow in; the router never re-parses (E04). On failure the
    // service drops here with its lock released; directories are never
    // removed (they may hold foreign runs), only the lock fd is released.
    let app = build_router(&state, &config, &validated_cors, rate_limit)?;
    let service = state.service.clone();
    // Install the SIGTERM handler before recovery runs or resident tasks
    // start: a broken signal setup must refuse the boot with nothing
    // running yet, instead of relying on abort-path cleanup after engines
    // already spawned (E05).
    #[cfg(unix)]
    let terminate = install_terminate_signal()
        .map_err(|error| anyhow::anyhow!("failed to install SIGTERM handler: {error}"))?;
    // All deployment policy was resolved before service creation and the
    // router is ready: recovered runs are admitted before the resident
    // maintenance tasks start, so the first GC and resumer pass cannot
    // race the recovery admission (E04).
    service.resume_recovered_runs().await;
    let mut tasks = ResidentTasks::start(&service, &shutdown, auto_gc);
    // Resident OTLP export: best-effort delivery, cancelled by the shared
    // shutdown token, never on the run execution path. Owned by
    // ResidentTasks so an outer-deadline timeout aborts and joins it like
    // every other resident (F01).
    if let Some(otlp) = otlp {
        tasks.spawn_otlp(super::otlp::run_exporter(Arc::clone(&state), otlp));
    }
    tracing::info!(service = %policy::default_service_name(), %actual_addr, "server listening");
    // Single shutdown initiation (E05): every path below converges on
    // `initiate_shutdown`, which cancels the single shared token owned by
    // the service (`mark_shutting_down` cancels this same token; the two
    // are one shutdown state, not two APIs). The signal future must not own
    // a service clone: axum runs it in a detached task that outlives an
    // aborted serve future, so a clone here would pin the run-store lock
    // forever and rebuilding on the same directory would fail after abort
    // (E05). Cancelling the shared token is exactly what
    // `mark_shutting_down` does, without the ownership.
    let shutdown_at_signal = shutdown.clone();
    // Normal operation is unbounded (F01): no timeout wraps the serve
    // future itself. The drain timeout starts only after the shutdown
    // signal fires. Before the signal the server runs indefinitely; after
    // the signal in-flight connections have `shutdown_drain` to drain
    // before shutdown proceeds anyway.
    let serve_grand = axum::serve(listener, app).with_graceful_shutdown(async move {
        #[cfg(unix)]
        shutdown_signal(terminate).await;
        #[cfg(not(unix))]
        shutdown_signal().await;
        // Completing this future stops accepting new connections and
        // starts the HTTP drain. Initiating shutdown here rejects new
        // mutating requests, closes SSE streams, and makes the service
        // refuse internal admissions from the same moment (E05).
        initiate_shutdown(&shutdown_at_signal);
    });
    // `WithGracefulShutdown` is `IntoFuture` but not `Future`: wrap it so
    // `select!` and the drain-only `timeout` share one future object.
    let mut serve_future = Box::pin(async move { serve_grand.await });
    let serve_result: std::io::Result<()> = tokio::select! {
        result = &mut serve_future => result,
        _ = shutdown.cancelled() => {
            // Shutdown was initiated externally (e.g. service-level
            // cancellation) before the OS signal future completed.
            // The graceful-shutdown future is now draining; bound only
            // the drain phase from this point.
            match tokio::time::timeout(shutdown_drain, serve_future).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        drain_secs = shutdown_drain.as_secs(),
                        "HTTP drain exceeded its timeout; proceeding to shutdown"
                    );
                    Ok(())
                }
            }
        }
    };
    // Serving returned, so the shutdown signal fired (or the listener
    // failed): converge on the same shutdown state through the single
    // initiation above (E05). `mark_shutting_down` would cancel this same
    // token; calling the single helper keeps one initiation path.
    initiate_shutdown(&shutdown);
    // HTTP is drained. Stop the resident execution sources and converge
    // active runs and external cleanups under one global deadline, so the
    // outer guarantee also covers resident join time. Resident join and run
    // convergence run concurrently: serializing them would let one consume
    // the other's settlement budget (E05). Failures and deadline
    // overruns are returned to the embedding host instead of being logged
    // and swallowed (E05).
    let outcome = tokio::time::timeout(shutdown_deadline, async move {
        let mut tasks = tasks;
        let (tasks_result, runs_result) =
            tokio::join!(tasks.shutdown(), service.shutdown_active_runs());
        // H04: both sides are done here (join! completed), so the abort
        // guard is disarmed exactly once. The timeout path drops `tasks`
        // without reaching this line, preserving forced recovery.
        tasks.disarm_engine_cleanup();
        tasks_result?;
        runs_result.map_err(anyhow::Error::from)
    })
    .await;
    // Report both failures when serving and shutdown both fail: returning
    // early on the serve error would silently drop the shutdown error
    // (E05).
    let serve_error = serve_result.err();
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if let Some(serve_error) = serve_error {
                anyhow::bail!("server failed: {serve_error}; shutdown also failed: {error}");
            }
            return Err(error);
        }
        Err(_) => {
            // The timed-out future is dropped here, which drops
            // ResidentTasks: its Drop aborts resident handles and every
            // engine task, so nothing is left detached after the deadline
            // (E05).
            if let Some(serve_error) = serve_error {
                anyhow::bail!(
                    "server failed: {serve_error}; graceful shutdown exceeded its {}s deadline",
                    shutdown_deadline.as_secs()
                );
            }
            anyhow::bail!(
                "graceful shutdown exceeded its {}s deadline",
                shutdown_deadline.as_secs()
            )
        }
    }
    if let Some(serve_error) = serve_error {
        return Err(serve_error.into());
    }
    Ok(actual_addr)
}

/// Global bound on graceful shutdown after HTTP has drained. Active-run
/// settling and container cleanup have their own internal bounds; this is
/// the outer guarantee returned to the embedding host. Tests override it
/// via [`serve_with_resolved_policy_and_deadline`] so the timeout path runs
/// fast (E05).
/// Total shutdown bound is `DRAIN_TIMEOUT + SHUTDOWN_DEADLINE` = 180 s
/// (E05): the outer deadline starts after the HTTP drain completes, so a
/// wedged drain delays settlement by design (it is cut after 30 s with a
/// warning) and the total time from signal to exit never exceeds 180 s.
/// Documented in `docs/operations.md`.
const SHUTDOWN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(150);

/// Bound on the HTTP drain phase itself: after the shutdown signal fires,
/// in-flight connections have this long to drain before shutdown proceeds
/// to resident-task and run settlement anyway (E05). Without it a wedged
/// drain connection delays the outer deadline start indefinitely. Documented
/// in `docs/operations.md` and `docs/http-server-guide.md`.
/// Total bound with [`SHUTDOWN_DEADLINE`] is 180 s from signal to exit.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Total bound from shutdown signal to process exit: drain plus outer
/// settlement (E05). Kept as a named constant so the 180 s total cannot
/// drift when either phase is retuned. Linked to the two phases by the
/// compile-time assertion below (E04) so release builds check it too, not
/// only debug builds and tests.
const TOTAL_SHUTDOWN_BOUND: std::time::Duration = std::time::Duration::from_secs(180);

// Compile-time linkage (E04): the total must stay drain + outer. A retune
// of either phase without the total fails the build, not just a test.
const _: () = assert!(
    TOTAL_SHUTDOWN_BOUND.as_secs() == DRAIN_TIMEOUT.as_secs() + SHUTDOWN_DEADLINE.as_secs()
);

/// Single shutdown initiation for the serve path (E05): cancels the shared
/// token owned by the service. `LocalService::mark_shutting_down`
/// cancels this same token; serve calls only this helper so the token is
/// never initiated through two APIs that could drift. Idempotent: calling
/// twice (signal + post-serve) converges on one cancelled state.
fn initiate_shutdown(shutdown: &CancellationToken) {
    shutdown.cancel();
}

/// Owns the server's resident maintenance tasks. `Drop` aborts them and
/// every engine task, so an aborted or panicking serve future cannot leak
/// resumers or runs that hold service clones and the run-store lock (E05).
struct ResidentTasks {
    handles: Vec<tokio::task::JoinHandle<Result<(), service::ServiceError>>>,
    /// Abort handles for every resident task, retained for the lifetime of
    /// this struct (F01). Waiters move the original `JoinHandle` inside so
    /// `handles` is drained during graceful shutdown; aborting only the
    /// waiter would drop the inner handle (detach) without stopping the
    /// original task. These abort handles let `Drop` stop the originals
    /// even after the waiters were aborted by an outer-deadline timeout.
    abort_handles: Vec<tokio::task::AbortHandle>,
    /// Grace waiters spawned by `shutdown`. Kept on the struct (not in a
    /// local) so `Drop` can abort them on the abort path: dropping a bare
    /// `JoinSet` would detach waiter tasks that own resident handles and
    /// leak them past an outer-deadline timeout (E05).
    waiters: tokio::task::JoinSet<Result<(), String>>,
    /// Service handle for abort-path engine cleanup. Unit tests that only
    /// exercise handle bookkeeping leave this empty (E05).
    service: Option<LocalService>,
    /// OTLP exporter handle owned like every other resident (F01). Kept
    /// separate because its output type differs from the service tasks.
    otlp_handle: Option<tokio::task::JoinHandle<()>>,
    otlp_abort: Option<tokio::task::AbortHandle>,
}

impl ResidentTasks {
    fn start(service: &LocalService, shutdown: &CancellationToken, auto_gc: bool) -> Self {
        let mut handles = Vec::new();
        if let Some(handle) = service.start_shared_store_recovery(shutdown.clone()) {
            handles.push(handle);
        }
        handles.push(service.start_queued_resumer(shutdown.clone()));
        if let Some(handle) = service.start_catalog_refresh(shutdown.clone()) {
            handles.push(handle);
        }
        if auto_gc && let Some(handle) = service.start_retention_gc(shutdown.clone()) {
            handles.push(handle);
        }
        let abort_handles = handles.iter().map(|h| h.abort_handle()).collect();
        Self {
            handles,
            abort_handles,
            waiters: tokio::task::JoinSet::new(),
            service: Some(service.clone()),
            otlp_handle: None,
            otlp_abort: None,
        }
    }

    /// Takes ownership of the OTLP exporter task so shutdown and abort
    /// paths join it like every other resident (F01).
    fn spawn_otlp(&mut self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        let handle = tokio::spawn(future);
        self.otlp_abort = Some(handle.abort_handle());
        self.otlp_handle = Some(handle);
    }

    /// H04: borrows instead of consuming so the abort guard (`Drop`)
    /// lives until BOTH resident join and run settlement complete. The
    /// caller disarms engine cleanup only after the join (see
    /// [`Self::disarm_engine_cleanup`]); dropping `self` after a lone
    /// resident join must not abort still-settling engine tasks.
    async fn shutdown(&mut self) -> Result<()> {
        // The token was already cancelled when the signal fired, so the
        // loops exit at their next select. Await them instead of aborting:
        // aborting mid-iteration could strand a run that the task just
        // admitted (E05). Every task is awaited concurrently with its own
        // 5 s grace period, so one wedged task cannot delay the rest or
        // the run convergence that follows: the total wait is ~5 s, never
        // 5 s per task (E05). A wedged task is aborted after its grace so
        // no detached task survives shutdown. A panicking task or a task
        // that ended with Err (for example the GC task after bounded
        // retries) is returned as an error so the outer shutdown outcome
        // cannot claim clean success while swallowing the failure (E05).
        // Waiters live in `self.waiters` so an outer-deadline timeout that
        // drops this future aborts them via `Drop` instead of detaching
        // them with resident handles inside (E05).
        for mut handle in self.handles.drain(..) {
            self.waiters.spawn(async move {
                match tokio::time::timeout(std::time::Duration::from_secs(5), &mut handle).await {
                    Err(_) => {
                        handle.abort();
                        // Confirm termination so no detached task keeps a
                        // service clone (and the run-store lock) alive
                        // after the grace period (F01).
                        let _ = handle.await;
                        tracing::warn!("resident task exceeded the shutdown grace period; aborted");
                        Err("resident task exceeded the shutdown grace period".to_string())
                    }
                    Ok(Err(join_error)) => {
                        tracing::warn!(%join_error, "resident task failed during shutdown");
                        Err(format!(
                            "resident task failed during shutdown: {join_error}"
                        ))
                    }
                    Ok(Ok(Err(task_error))) => {
                        tracing::warn!(%task_error, "resident task ended with an error");
                        Err(format!("resident task ended with an error: {task_error}"))
                    }
                    Ok(Ok(Ok(()))) => Ok(()),
                }
            });
        }
        if let Some(mut otlp) = self.otlp_handle.take() {
            self.waiters.spawn(async move {
                match tokio::time::timeout(std::time::Duration::from_secs(5), &mut otlp).await {
                    Err(_) => {
                        otlp.abort();
                        let _ = otlp.await;
                        tracing::warn!("OTLP exporter exceeded the shutdown grace period; aborted");
                        Err("OTLP exporter exceeded the shutdown grace period".to_string())
                    }
                    Ok(Err(join_error)) if !join_error.is_cancelled() => {
                        tracing::warn!(%join_error, "OTLP exporter failed during shutdown");
                        Err(format!(
                            "OTLP exporter failed during shutdown: {join_error}"
                        ))
                    }
                    _ => Ok(()),
                }
            });
        }
        let mut failures: Vec<String> = Vec::new();
        while let Some(outcome) = self.waiters.join_next().await {
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(detail)) => {
                    failures.push(detail);
                }
                Err(join_error) => {
                    tracing::warn!(%join_error, "resident task waiter failed during shutdown");
                    failures.push(format!(
                        "resident task waiter failed during shutdown: {join_error}"
                    ));
                }
            }
        }
        // Every failure is reported, not just the first: concurrent task
        // failures must all reach the embedding host (E05).
        match failures.len() {
            0 => Ok(()),
            1 => Err(anyhow::anyhow!(failures.pop().unwrap_or_default())),
            count => Err(anyhow::anyhow!(
                "{count} resident tasks failed during shutdown: {}",
                failures.join("; ")
            )),
        }
    }

    /// H04: disarms the abort-path engine cleanup after BOTH resident join
    /// and run settlement complete. Called once by the shutdown owner after
    /// the `join!`; the abort path (outer deadline, serve abort) never calls
    /// it, so forced recovery still fires there.
    fn disarm_engine_cleanup(&mut self) {
        self.service = None;
    }
}

impl Drop for ResidentTasks {
    fn drop(&mut self) {
        // Abort-path cleanup (H04): the graceful path disarms via
        // `disarm_engine_cleanup` only after BOTH resident join and run
        // settlement complete, so this fires solely on the abort path
        // (serve abort/panic, including an outer-deadline timeout which
        // drops the shutdown future holding this struct). Without it
        // the per-run engine tasks keep their service clones (and the
        // run-store lock) alive forever, and rebuilding on the same
        // directory fails (E05). Both the waiter set and the resident
        // handles are aborted explicitly: dropping a `JoinSet` alone
        // would detach them.
        if let Some(service) = &self.service {
            service.abort_all_engine_tasks();
        }
        self.waiters.abort_all();
        // Abort the original resident tasks via retained abort handles
        // (F01): `handles` may already be drained into waiters, and
        // aborting only the waiter would detach the original task.
        for abort in &self.abort_handles {
            abort.abort();
        }
        for handle in &self.handles {
            handle.abort();
        }
        if let Some(abort) = &self.otlp_abort {
            abort.abort();
        }
        if let Some(handle) = &self.otlp_handle {
            handle.abort();
        }
    }
}

pub(crate) fn build_router(
    state: &Arc<AppState>,
    config: &ServerConfig,
    validated_cors: &[HeaderValue],
    rate_limit: Option<RateLimitPolicy>,
) -> Result<Router> {
    // Defense in depth for direct callers that skip `resolve_server_policy`:
    // zero explicit-max knobs refuse here (post-service, pre-recovery) with
    // zero recovery, matching the boot-policy refusal (E04). The resolved
    // path already refused these before service construction; this guards
    // direct `build_router` use via the single shared helper above.
    reject_zero_explicit_max(config).map_err(anyhow::Error::msg)?;
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/api/openapi.json", get(openapi))
        .route("/api/generators", get(list_generators))
        .route("/api/llm/catalog", get(llm_catalog))
        .route("/api/generators/{id}", get(describe_generator))
        .route(
            "/api/generators/{id}/assets/{*path}",
            get(read_generator_asset),
        )
        .route("/api/mcp/servers", get(list_mcp_servers))
        .route(
            "/api/mcp/servers/{id}/authorization",
            axum::routing::post(start_mcp_authorization).delete(clear_mcp_authorization),
        )
        .route(
            "/api/mcp/servers/{id}/authorization/pending",
            axum::routing::delete(cancel_pending_mcp_authorization),
        )
        .route("/api/mcp/oauth/callback", get(complete_mcp_authorization))
        .route("/api/runs", get(list_runs).post(start_run))
        .route(
            "/api/runs/{id}",
            get(run_snapshot)
                .post(cancel_run_from_path)
                .delete(delete_run),
        )
        .route("/api/runs/{id}/fork", axum::routing::post(fork_run))
        .route("/api/runs/{id}/questions/{qid}", put(answer_run))
        .route("/api/runs/{id}/confirmations/{cid}", put(confirm_run))
        .route("/api/runs/{id}/events", get(run_events))
        .route("/api/runs/{id}/artifacts", get(run_artifacts))
        .route("/api/runs/{id}/artifacts.zip", get(read_artifacts_zip))
        .route("/api/runs/{id}/bundle", get(read_run_bundle))
        .route("/api/runs/{id}/artifacts/{*path}", get(read_artifact))
        .route("/api/runs/{id}/journal", get(read_journal))
        .route("/api/runs/{id}/metrics", get(read_cost_metrics))
        .fallback(dispatch_fallback)
        // Axum `Router::layer` is onion ordered: the last `layer` call is
        // the outermost middleware and runs first. Auth is layered before
        // the shutdown gate so the shutdown gate is outermost and runs
        // before auth. Shutdown refusal therefore precedes authentication,
        // so draining returns 503 without leaking timing about credentials
        // (E05). The rate limit layer is layered after auth and immediately
        // inside the shutdown gate: draining still answers 503 before the
        // limiter can answer 429, while the limiter covers unauthenticated
        // floods with a shared anonymous bucket and splits only verified
        // identity (F10).
        .layer(axum_middleware::from_fn(reject_unsafe_generator_asset_path))
        .layer(axum_middleware::from_fn(security_headers_middleware))
        .layer(axum_middleware::from_fn_with_state(
            Arc::clone(state),
            require_api_auth,
        ));
    let app = match rate_limit {
        Some(policy) => {
            let limiter = RateLimiter::with_expected_digest(policy, state.api_token_digest);
            app.layer(axum_middleware::from_fn_with_state(
                Arc::new(limiter),
                enforce_rate_limit,
            ))
        }
        None => app,
    };
    let mut app = app
        .layer(axum_middleware::from_fn_with_state(
            Arc::clone(state),
            reject_mutating_requests_during_shutdown,
        ))
        .with_state(Arc::clone(state));
    // None means no mechanistic limit: disable Axum's 2 MiB default so the
    // documented `Explicit max only. Omitted means no mechanistic limit.`
    // holds for the JSON extractor as well as FileValue limits (A12).
    match config.max_request_bytes {
        Some(max_request_bytes) => {
            app = app.layer(DefaultBodyLimit::max(max_request_bytes));
        }
        None => {
            app = app.layer(DefaultBodyLimit::disable());
        }
    }
    // The validated origins from the single parse above flow in; an empty
    // list means no layer. The router never parses strings itself (E04).
    let app = if !validated_cors.is_empty() {
        apply_cors_layer(app, validated_cors)?
    } else {
        app
    };
    Ok(app)
}

/// Single CORS parser: strings become validated header values exactly once
/// per boot or router build, and the validated values flow into layer
/// construction so no second parse exists (E04). Validation is semantic,
/// not just header syntax: origins must be `http(s)://host[:port]` without
/// path, query, or fragment, so a syntactically valid header like
/// `https://example.com/evil` cannot boot (E04).
pub(crate) fn parse_cors_origins(cors_origins: &[String]) -> Result<Vec<HeaderValue>, String> {
    #[cfg(not(feature = "server-cors"))]
    if !cors_origins.is_empty() {
        return Err(
            "CORS origins are configured, but this build disables the `server-cors` cargo feature"
                .to_string(),
        );
    }
    let mut validated = Vec::with_capacity(cors_origins.len());
    for origin in cors_origins {
        origin
            .parse::<HeaderValue>()
            .map_err(|error| format!("invalid CORS origin `{origin}`: {error}"))?;
        // Semantic check beyond header syntax (E04).
        let lower = origin.to_ascii_lowercase();
        let rest = lower
            .strip_prefix("http://")
            .or_else(|| lower.strip_prefix("https://"))
            .ok_or_else(|| {
                format!("invalid CORS origin `{origin}`: must start with http:// or https://")
            })?;
        // Semantic check beyond header syntax (E04). Userinfo is refused
        // so `https://evil@example.com` cannot smuggle credentials or
        // mislead an allowlist comparison.
        if rest.contains('@') {
            return Err(format!(
                "invalid CORS origin `{origin}`: userinfo is not allowed"
            ));
        }
        if rest.is_empty()
            || rest.contains('/')
            || rest.contains('?')
            || rest.contains('#')
            || rest.contains(' ')
        {
            return Err(format!(
                "invalid CORS origin `{origin}`: must be scheme + host[:port] without path"
            ));
        }
        // A present port must be numeric; `https://host:abc` is refused
        // instead of passed to the CORS layer (E04). Bracketed IPv6
        // literals (`[::1]`, `[::1]:8080`) are exempt from the suffix
        // check up to the closing bracket.
        let port_part = if let Some(stripped) = rest.strip_prefix('[') {
            match stripped.find(']') {
                Some(end) => {
                    let after = &stripped[end + 1..];
                    after.strip_prefix(':')
                }
                None => {
                    return Err(format!(
                        "invalid CORS origin `{origin}`: malformed IPv6 literal"
                    ));
                }
            }
        } else if rest.contains(':') {
            rest.rsplit(':').next()
        } else {
            None
        };
        if let Some(port) = port_part
            && (port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(format!(
                "invalid CORS origin `{origin}`: port must be numeric"
            ));
        }
        // Normalize to lowercase so the layer compares against the
        // lowercase origin browsers actually send (E04). Duplicates are
        // collapsed after normalization so the allowlist holds each origin
        // once: a duplicated flag/env entry cannot widen layer semantics
        // (E04).
        let normalized = lower
            .parse::<HeaderValue>()
            .map_err(|error| format!("invalid CORS origin `{origin}`: {error}"))?;
        if !validated.contains(&normalized) {
            validated.push(normalized);
        }
    }
    Ok(validated)
}

#[cfg(feature = "server-cors")]
fn apply_cors_layer(app: Router, validated_cors: &[HeaderValue]) -> Result<Router> {
    // No parsing here: the caller parsed once and passes validated values
    // (E04).
    Ok(app.layer(
        CorsLayer::new()
            .allow_origin(tower_http::cors::AllowOrigin::list(validated_cors.to_vec()))
            .allow_headers([
                header::AUTHORIZATION,
                header::CONTENT_TYPE,
                header::HeaderName::from_static(IDEMPOTENCY_HEADER),
                header::HeaderName::from_static("last-event-id"),
                header::IF_NONE_MATCH,
                header::IF_RANGE,
                header::RANGE,
            ])
            .expose_headers([
                header::ETAG,
                header::LOCATION,
                header::RETRY_AFTER,
                header::CONTENT_DISPOSITION,
                header::CONTENT_RANGE,
                header::ACCEPT_RANGES,
            ])
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ]),
    ))
}

#[cfg(not(feature = "server-cors"))]
fn apply_cors_layer(_app: Router, validated_cors: &[HeaderValue]) -> Result<Router> {
    if !validated_cors.is_empty() {
        anyhow::bail!(
            "CORS origins are configured, but this build disables the `server-cors` cargo feature"
        );
    }
    Ok(_app)
}

/// Rejects new mutating API work once graceful shutdown has started so the
/// drain phase cannot admit runs the shutdown snapshot never saw (E05).
/// The refusal uses the JSON problem shape like every other API error, so
/// clients never branch on content type during drain (E05).
async fn reject_mutating_requests_during_shutdown(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum_middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    if state.shutdown.is_cancelled()
        && matches!(
            request.method(),
            &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE
        )
    {
        return super::error::ApiHttpError::service_unavailable("server is shutting down")
            .into_response();
    }
    next.run(request).await
}

/// Installs the SIGTERM handler up front so a broken signal setup fails
/// the boot instead of silently degrading graceful shutdown later (E05).
#[cfg(unix)]
fn install_terminate_signal() -> Result<tokio::signal::unix::Signal, std::io::Error> {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
}

/// Waits for a shutdown signal. Stopping new work, stopping resident tasks,
/// and settling active runs are the caller's ordered responsibility, so the
/// library never calls `process::exit` and embedded hosts observe graceful
/// HTTP drain (A09/E05). A Ctrl-C wait failure still proceeds to shutdown
/// (fail-safe: stopping is always safer than hanging).
#[cfg(unix)]
pub(crate) async fn shutdown_signal(mut terminate: tokio::signal::unix::Signal) {
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::error!(%error, "failed to wait for Ctrl-C");
            }
        }
        _ = terminate.recv() => {}
    }
}

/// Waits for a shutdown signal (non-Unix variant of [`shutdown_signal`]).
/// Platform matrix (E05, documented in `docs/operations.md`):
/// Unix waits for Ctrl-C or SIGTERM; Windows waits for Ctrl-C plus the
/// console/service controls tokio exposes (`ctrl_close`, `ctrl_break`)
/// as the SIGTERM equivalents; other
/// targets wait for Ctrl-C only. A wait failure still proceeds to shutdown
/// (fail-safe: stopping is always safer than hanging).
#[cfg(not(unix))]
pub(crate) async fn shutdown_signal() {
    #[cfg(windows)]
    {
        let close = match tokio::signal::windows::ctrl_close() {
            Ok(signal) => Some(signal),
            Err(error) => {
                tracing::error!(%error, "failed to install Ctrl-Close handler");
                None
            }
        };
        let brk = match tokio::signal::windows::ctrl_break() {
            Ok(signal) => Some(signal),
            Err(error) => {
                tracing::error!(%error, "failed to install Ctrl-Break handler");
                None
            }
        };
        // Installed handlers race below in `windows_shutdown_signal`.
        windows_shutdown_signal(close, brk).await;
    }
    #[cfg(not(windows))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to wait for Ctrl-C");
        }
    }
}

/// Windows SIGTERM-equivalent wait (E05): Ctrl-C plus console/service
/// controls. Each installed handler races; a missing handler (install
/// failure above) is skipped, never a hang. Separated for unit clarity;
/// `shutdown_signal` owns installation, this owns the wait.
#[cfg(all(not(unix), windows))]
async fn windows_shutdown_signal(
    mut close: Option<tokio::signal::windows::CtrlClose>,
    mut brk: Option<tokio::signal::windows::CtrlBreak>,
) {
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::error!(%error, "failed to wait for Ctrl-C");
            }
        }
        _ = async {
            match close.as_mut() {
                Some(signal) => { signal.recv().await; }
                None => std::future::pending::<()>().await,
            }
        } => {}
        _ = async {
            match brk.as_mut() {
                Some(signal) => { signal.recv().await; }
                None => std::future::pending::<()>().await,
            }
        } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::middleware::sha256_bytes;
    use crate::test_state;

    /// Boots a service that admits exactly one active run, so a queueing or
    /// drain assertion observes the concurrency bound instead of racing the
    /// default. Exclusive store mode, no explicit providers file, and the
    /// default deployment policy match `crate::test_service`; only the
    /// active-run slot differs.
    ///
    /// The runs directory is also the run store, so a refused boot means a
    /// contended lock. The raw `Result` is returned rather than unwrapped so
    /// a caller that polls for the release can retry instead of panicking.
    fn try_single_run_service(
        generators_dir: &camino::Utf8Path,
        runs_dir: &camino::Utf8Path,
    ) -> Result<LocalService, service::ServiceError> {
        LocalService::with_generator_roots_policy_and_store_mode(
            vec![generators_dir.to_owned()],
            runs_dir.to_owned(),
            None,
            1,
            policy::DEFAULT_MAX_TRACKED_RUNS,
            service::RunStoreMode::Exclusive,
            service::ServiceDeploymentPolicy::default(),
        )
    }

    /// `try_single_run_service` for a test that expects the boot to succeed.
    fn single_run_service(
        generators_dir: &camino::Utf8Path,
        runs_dir: &camino::Utf8Path,
    ) -> LocalService {
        try_single_run_service(generators_dir, runs_dir).expect("service should initialize")
    }

    /// Acquires the exclusive run store, waiting for a refused boot to release
    /// it.
    ///
    /// A dropped `AppState` frees the lock only when the last clone of the
    /// service is gone, and a task that was already resident observes
    /// cancellation asynchronously, so the release is waited for instead of
    /// raced. The constructor is its own probe, so this asserts the real
    /// precondition and still fails when the store is never released.
    async fn acquire_store_after_release(
        generators: &camino::Utf8Path,
        runs: &camino::Utf8Path,
    ) -> LocalService {
        for _ in 0..200 {
            match try_single_run_service(generators, runs) {
                Ok(service) => return service,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
        panic!("the refused boot must release the runs directory `{runs}`");
    }

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04).
    struct TempGuard(camino::Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    #[tokio::test]
    async fn shutdown_gate_precedes_authentication() {
        // E05: draining must return 503 for unauthenticated mutating
        // requests, never 401, so shutdown timing does not leak credential
        // validity.
        use tower::ServiceExt as _;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e05-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let service = single_run_service(&generators, &runs);
        let state = Arc::new(test_state(
            service,
            runs.clone(),
            Some(sha256_bytes("secret")),
            shutdown.clone(),
        ));
        let config = ServerConfig {
            generators_dir: generators.clone(),
            providers_path: None,
            extra_generators_dirs: Vec::new(),
            runs_dir: runs.clone(),
            max_active_runs: 1,
            max_tracked_runs: policy::DEFAULT_MAX_TRACKED_RUNS,
            run_store_mode: service::RunStoreMode::Exclusive,
            cors_origins: Vec::new(),
            api_token: Some("secret".into()),
            max_request_bytes: None,
            max_artifact_bytes: None,
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: None,
        };
        let validated_cors =
            parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
        let app =
            build_router(&state, &config, &validated_cors, None).expect("router should build");
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/runs")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{}"))
            .expect("request should build");
        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("shutdown request should respond");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "unauthenticated mutating request during drain must be 503, not 401"
        );
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/runs")
            .header("content-type", "application/json")
            .header("authorization", "Bearer wrong")
            .body(axum::body::Body::from("{}"))
            .expect("request should build");
        let response = app
            .oneshot(request)
            .await
            .expect("shutdown request should respond");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "wrong credential during drain must still be 503, not 401"
        );
    }

    #[tokio::test]
    async fn shutdown_gate_covers_the_full_mutating_set() {
        // E05/Q3: every mutating route (start, fork, answer, confirm,
        // cancel, delete) must 503 during drain, including idempotent
        // replays that would otherwise report Ok without new work. Reads
        // stay admissible (never 503 for GET). The gate runs before auth
        // and handlers, so dummy ids still 503 without touching runs.
        use tower::ServiceExt as _;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e05-gate-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let service = single_run_service(&generators, &runs);
        let state = Arc::new(test_state(service, runs.clone(), None, shutdown.clone()));
        let config = boot_config(&generators, &runs);
        let validated_cors =
            parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
        let app =
            build_router(&state, &config, &validated_cors, None).expect("router should build");
        // (method, uri, body) for every mutating route.
        let mutating = vec![
            (Method::POST, "/api/runs".to_string(), Some("{}")),
            (Method::POST, "/api/runs/x/fork".to_string(), Some("{}")),
            (
                Method::PUT,
                "/api/runs/x/questions/q".to_string(),
                Some(r#"{"values":{}}"#),
            ),
            (
                Method::PUT,
                "/api/runs/x/confirmations/c".to_string(),
                Some(r#"{"decision":"approve"}"#),
            ),
            (Method::POST, "/api/runs/x:cancel".to_string(), None),
            (Method::DELETE, "/api/runs/x".to_string(), None),
        ];
        for (method, uri, body) in mutating {
            let mut builder = axum::http::Request::builder()
                .method(method.clone())
                .uri(&uri);
            if body.is_some() {
                builder = builder.header("content-type", "application/json");
            }
            let request = builder
                .body(axum::body::Body::from(body.unwrap_or_default()))
                .expect("request should build");
            let response = app
                .clone()
                .oneshot(request)
                .await
                .expect("drain request should respond");
            assert_eq!(
                response.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "{method} {uri} must 503 during drain"
            );
        }
        // Reads stay admissible: GET is never gated to 503 (the run may
        // 404, but the gate must not convert it to 503).
        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/runs/x")
            .body(axum::body::Body::empty())
            .expect("request should build");
        let response = app
            .oneshot(request)
            .await
            .expect("read during drain should respond");
        assert_ne!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "reads must stay admissible during drain"
        );
    }

    #[test]
    fn cors_rejects_path_query_and_non_http_origins() {
        // E04: semantic validation beyond header syntax.
        for bad in [
            "https://example.com/evil",
            "https://example.com?q=1",
            "https://example.com#x",
            "ftp://example.com",
            "example.com",
            "https://",
            "https://evil@example.com",
            "https://example.com:abc",
            "https://[::1",
            "HTTPS://EXAMPLE.COM/EVIL",
        ] {
            assert!(
                parse_cors_origins(&[bad.to_string()]).is_err(),
                "must reject {bad}"
            );
        }
        // Without the server-cors feature every configured origin is
        // refused (the layer cannot be built); with it, well-formed
        // origins parse and only malformed ones fail.
        #[cfg(not(feature = "server-cors"))]
        assert!(
            parse_cors_origins(&["https://example.com:8080".to_string()])
                .expect_err("origins must be refused without the server-cors feature")
                .contains("server-cors"),
            "refusal must name the disabled feature"
        );
        #[cfg(feature = "server-cors")]
        assert!(parse_cors_origins(&["https://example.com:8080".to_string()]).is_ok());
        // Uppercase scheme/host parses (case-insensitive) and duplicates
        // collapse after normalization so the layer holds each origin once
        // (E04).
        #[cfg(feature = "server-cors")]
        assert!(parse_cors_origins(&["HTTPS://EXAMPLE.COM".to_string()]).is_ok());
        #[cfg(feature = "server-cors")]
        assert_eq!(
            parse_cors_origins(&[
                "https://example.com".to_string(),
                "https://example.com".to_string()
            ])
            .expect("duplicates should collapse")
            .len(),
            1
        );
        #[cfg(feature = "server-cors")]
        assert_eq!(
            parse_cors_origins(&[
                "HTTPS://EXAMPLE.COM".to_string(),
                "https://example.com".to_string(),
                "https://example.com:8080".to_string(),
            ])
            .expect("case-duplicates should collapse")
            .len(),
            2
        );
    }

    #[tokio::test]
    async fn resident_task_panic_fails_shutdown() {
        // E05: a panicking resident task must fail shutdown as an error,
        // never vanish as a clean success.
        let handle = tokio::spawn(async {
            panic!("resident task panic");
            #[allow(unreachable_code)]
            Ok::<(), service::ServiceError>(())
        });
        let mut tasks = ResidentTasks {
            handles: vec![handle],
            abort_handles: vec![],
            waiters: tokio::task::JoinSet::new(),
            service: None,
            otlp_handle: None,
            otlp_abort: None,
        };
        let error = tasks
            .shutdown()
            .await
            .expect_err("a panicking task must fail shutdown");
        assert!(
            error.to_string().contains("resident task failed"),
            "the panic must propagate: {error}"
        );
    }

    #[tokio::test]
    async fn wedged_tasks_do_not_delay_each_other() {
        // E05: every resident task gets its own concurrent 5 s grace, so
        // two wedged tasks settle in ~5 s total, never ~10 s sequential.
        // Both must be aborted and reported as one error.
        let first = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok::<(), service::ServiceError>(())
        });
        let second = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok::<(), service::ServiceError>(())
        });
        let mut tasks = ResidentTasks {
            handles: vec![first, second],
            abort_handles: vec![],
            waiters: tokio::task::JoinSet::new(),
            service: None,
            otlp_handle: None,
            otlp_abort: None,
        };
        let started = tokio::time::Instant::now();
        let error = tasks
            .shutdown()
            .await
            .expect_err("wedged tasks must fail shutdown");
        assert!(
            error.to_string().contains("grace period"),
            "the timeout must be reported: {error}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(9),
            "concurrent grace must bound the total wait: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn abort_path_releases_engine_tasks_and_the_store_lock() {
        // E05: aborting the serve future must not strand per-run engine
        // tasks holding service clones and the run-store lock: after
        // abort-all plus drop, the same directory rebuilds cleanly.
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e05-abort-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(generators.join("slow")).expect("slow generator dir");
        std::fs::write(
            generators.join("slow/qcg.toml"),
            r#"
[generator]
id = "slow"
name = "Slow"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "abort test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest");
        let service = single_run_service(&generators, &runs);
        let run_id = service
            .start_run(api::StartRun {
                generator_id: "slow".into(),
                inputs: Default::default(),
                ..Default::default()
            })
            .await
            .expect("slow run should start");
        // Wait until the engine task is live (not merely registered).
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let live = service
                    .snapshot(run_id.clone())
                    .await
                    .is_ok_and(|snapshot| snapshot.state == api::RunStatus::Running);
                if live {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the engine task should go live");
        service.abort_all_engine_tasks();
        drop(service);
        // Bounded polling wait (E05): a contended skip inside the abort
        // path is not a true leak, and the two are indistinguishable
        // without waiting — the reacquire below is the assertion, the
        // sleep is only its settle window, not a latency bound.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        try_single_run_service(&generators, &runs)
            .expect("the run-store lock must be reacquirable after abort cleanup");
    }

    #[tokio::test]
    async fn aborted_serve_future_releases_the_store_lock() {
        // E05: aborting `serve_with_listener` itself (not graceful
        // shutdown) must still free the run-store lock via drop cleanup,
        // so an embedding host can rebuild on the same directory.
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e05-serve-abort-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have an address");
        let config = ServerConfig {
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
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: None,
        };
        let server = tokio::spawn(serve_with_listener(config, listener));
        // Wait until the listener accepts: the serve future owns the
        // service (and its store lock) from here on. The probe uses
        // `Connection: close` and drains to EOF so no idle keep-alive
        // connection of ours pins the service after the abort below.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            loop {
                if let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await {
                    let _ = stream
                        .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                        .await;
                    let mut body = Vec::new();
                    if stream.read_to_end(&mut body).await.is_ok() && !body.is_empty() {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the server should start listening");
        server.abort();
        let _ = server.await;
        // The abort drops every service clone synchronously with task
        // teardown except detached HTTP connection tasks, which release
        // theirs as they observe EOF. Poll for the reacquire so a still
        // draining probe connection cannot flake the assertion, while a
        // genuine leak still fails loudly at the deadline (E05).
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                match try_single_run_service(&generators, &runs) {
                    Ok(_) => break,
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .expect("the run-store lock must be reacquirable after aborting serve");
    }

    #[tokio::test]
    async fn dropped_resident_tasks_abort_wedged_loops() {
        // E05: dropping the task owner (panic/abort path) must not leak a
        // detached maintenance loop: every owned handle is aborted.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            std::future::pending::<()>().await;
            let _ = done_tx.send(());
            Ok::<(), service::ServiceError>(())
        });
        let abort = handle.abort_handle();
        {
            let _tasks = ResidentTasks {
                handles: vec![handle],
                abort_handles: vec![abort],
                waiters: tokio::task::JoinSet::new(),
                service: None,
                otlp_handle: None,
                otlp_abort: None,
            };
            // Drop aborts the wedged loop here.
        }
        assert!(
            done_rx.await.is_err(),
            "the wedged loop must be aborted on drop, not detached"
        );
    }

    #[tokio::test]
    async fn abort_handles_survive_graceful_drain() {
        // F01: waiters move the original JoinHandles inside during graceful
        // shutdown; the retained abort handles let Drop stop the originals
        // even after an outer-deadline timeout aborts the waiters.
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok::<(), service::ServiceError>(())
        });
        let abort = handle.abort_handle();
        let mut tasks = ResidentTasks {
            handles: vec![handle],
            abort_handles: vec![abort.clone()],
            waiters: tokio::task::JoinSet::new(),
            service: None,
            otlp_handle: None,
            otlp_abort: None,
        };
        // Simulate the graceful path draining `handles` into waiters, then
        // an outer timeout dropping everything mid-wait.
        let drained = std::mem::take(&mut tasks.handles);
        assert_eq!(drained.len(), 1);
        drop(tasks);
        assert!(
            abort.is_finished() || {
                // Give the abort a moment to land; the task never exits alone.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                abort.is_finished()
            },
            "the original resident must stop via its retained abort handle"
        );
    }

    #[tokio::test]
    async fn h04_graceful_join_keeps_guard_until_both_sides_done() {
        // H04-01/H04-02: the abort guard must live across the
        // `join!(resident, runs)` in both orders and disarm only after both
        // complete. Borrowed shutdown (not consuming) is what keeps it alive.
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("h04-graceful-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir");
        let service = single_run_service(&generators, &runs);
        for order in ["resident-first", "run-first"] {
            let mut tasks = ResidentTasks {
                handles: vec![],
                abort_handles: vec![],
                waiters: tokio::task::JoinSet::new(),
                service: Some(service.clone()),
                otlp_handle: None,
                otlp_abort: None,
            };
            // Resident side is immediate; run side needs its grace period.
            // Both orders must complete without the guard firing mid-join:
            // the guard is still owned here after shutdown returns.
            let fake_run_settlement = async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok::<(), anyhow::Error>(())
            };
            if order == "resident-first" {
                let (tasks_result, runs_result) =
                    tokio::join!(tasks.shutdown(), fake_run_settlement);
                tasks_result.expect("resident join should succeed");
                runs_result.expect("run settlement should succeed");
            } else {
                let (runs_result, tasks_result) =
                    tokio::join!(fake_run_settlement, tasks.shutdown(),);
                runs_result.expect("run settlement should succeed");
                tasks_result.expect("resident join should succeed");
            }
            // Still owned after the join: disarm exactly once, then drop
            // must not abort (service cleared).
            assert!(
                tasks.service.is_some(),
                "H04 ({order}): guard must survive the join"
            );
            tasks.disarm_engine_cleanup();
            assert!(
                tasks.service.is_none(),
                "H04 ({order}): disarm must release engine cleanup"
            );
        }
        drop(service);
    }

    #[tokio::test]
    async fn h04_abort_path_still_armed_without_disarm() {
        // H04-03: the abort path (outer deadline, serve abort) never disarms,
        // so Drop must still fire forced recovery. Assert the guard stays
        // armed when shutdown is abandoned mid-wait and that Drop aborts
        // resident handles.
        let handle = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok::<(), service::ServiceError>(())
        });
        let abort = handle.abort_handle();
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("h04-abort-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir");
        let service = single_run_service(&generators, &runs);
        {
            let _tasks = ResidentTasks {
                handles: vec![handle],
                abort_handles: vec![abort.clone()],
                waiters: tokio::task::JoinSet::new(),
                service: Some(service.clone()),
                otlp_handle: None,
                otlp_abort: None,
            };
            // Drop without disarm = abort path.
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            abort.is_finished(),
            "H04-03: abort-path Drop must still force recovery"
        );
        drop(service);
    }

    #[tokio::test]
    async fn wedged_shutdown_surfaces_an_error_through_the_deadline_path() {
        // E05: a wedged resident task plus a live run must surface an error
        // through the outer deadline instead of reporting clean success.
        // Uses the real service, real resident tasks, and the real
        // shutdown_active_runs settlement (no mocks).
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e05-deadline-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(generators.join("slow")).expect("slow generator dir");
        std::fs::write(
            generators.join("slow/qcg.toml"),
            r#"
[generator]
id = "slow"
name = "Slow"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "deadline test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest");
        let service = single_run_service(&generators, &runs);
        let run_id = service
            .start_run(api::StartRun {
                generator_id: "slow".into(),
                inputs: Default::default(),
                ..Default::default()
            })
            .await
            .expect("slow run should start");
        // Wait until the engine task is live.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if service
                    .snapshot(run_id.clone())
                    .await
                    .is_ok_and(|snapshot| snapshot.state == api::RunStatus::Running)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the engine task should go live");
        // A wedged resident task that never exits on its own.
        let wedged = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok::<(), service::ServiceError>(())
        });
        let tasks = ResidentTasks {
            handles: vec![wedged],
            abort_handles: vec![],
            waiters: tokio::task::JoinSet::new(),
            service: Some(service.clone()),
            otlp_handle: None,
            otlp_abort: None,
        };
        service.mark_shutting_down();
        // The outer deadline from serve (short for the test) covers both
        // resident join time and run convergence: a wedged task plus a live
        // run cannot fit in 200 ms, so the timeout path must surface an
        // error instead of clean success (E05). Uses the same `join!`
        // composition as production (`tasks.shutdown` + `shutdown_active_runs`
        // concurrently under one deadline), never serial awaits that would
        // let one phase consume the other's budget.
        let outcome = tokio::time::timeout(std::time::Duration::from_millis(200), async move {
            let mut tasks = tasks;
            let (tasks_result, runs_result) =
                tokio::join!(tasks.shutdown(), service.shutdown_active_runs());
            tasks.disarm_engine_cleanup();
            tasks_result?;
            runs_result.map_err(anyhow::Error::from)
        })
        .await;
        match outcome {
            Err(_) => {}
            Ok(Err(_)) => {}
            Ok(Ok(())) => panic!("a wedged shutdown must surface an error, not clean success"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    // Async-aware mutex: tests mutate process-global environment, so the
    // guard is held across awaits by design and must not block the executor.
    static BOOT_ENV_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn boot_config(generators: &camino::Utf8PathBuf, runs: &camino::Utf8PathBuf) -> ServerConfig {
        ServerConfig {
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
            max_artifact_entries: None,
            max_asset_bytes: None,
            max_total_steps: None,
        }
    }

    #[tokio::test]
    async fn rate_limit_env_knobs_are_resolved_strictly() {
        if crate::tests::isolate_environment_test(
            "server::serve::tests::rate_limit_env_knobs_are_resolved_strictly",
        ) {
            return;
        }
        // E04: rate limit knobs are fail-closed. Zero, garbage, fractional,
        // negative, and empty values refuse boot, and so does a burst without
        // an rps; valid values freeze into the resolved policy.
        let _env_guard = BOOT_ENV_GUARD.lock().await;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e04-rate-limit-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let config = boot_config(&generators, &runs);
        let unset = |variable: &str| {
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe {
                std::env::remove_var(variable);
            }
        };
        let set = |variable: &str, value: &str| {
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe {
                std::env::set_var(variable, value);
            }
        };
        unset("RATE_LIMIT_RPS");
        unset("RATE_LIMIT_BURST");
        assert_eq!(
            resolve_server_policy(&config)
                .expect("unset rate limit should resolve")
                .rate_limit,
            None,
            "an unset rps must leave the server unthrottled"
        );
        for (rps, burst, part) in [
            (Some("0"), None, "RATE_LIMIT_RPS"),
            (Some("fast"), None, "RATE_LIMIT_RPS"),
            (Some(""), None, "RATE_LIMIT_RPS"),
            (Some("-1"), None, "RATE_LIMIT_RPS"),
            (Some("1.5"), None, "RATE_LIMIT_RPS"),
            (Some("10"), Some("0"), "RATE_LIMIT_BURST"),
            (Some("10"), Some("many"), "RATE_LIMIT_BURST"),
            (Some("10"), Some(""), "RATE_LIMIT_BURST"),
            (None, Some("5"), "RATE_LIMIT_RPS"),
        ] {
            match rps {
                Some(value) => set("RATE_LIMIT_RPS", value),
                None => unset("RATE_LIMIT_RPS"),
            }
            match burst {
                Some(value) => set("RATE_LIMIT_BURST", value),
                None => unset("RATE_LIMIT_BURST"),
            }
            let error = resolve_server_policy(&config)
                .expect_err("invalid rate limit env must refuse boot");
            assert!(
                error.contains("rate limit") && error.contains(part),
                "refusal must name {part}: {error}"
            );
        }
        set("RATE_LIMIT_RPS", "10");
        unset("RATE_LIMIT_BURST");
        assert_eq!(
            resolve_server_policy(&config)
                .expect("a positive rps should resolve")
                .rate_limit,
            Some(RateLimitPolicy {
                rps: 10,
                burst: 10,
                trusted_identity_header: None,
                max_identities: policy::DEFAULT_RATE_LIMIT_MAX_IDENTITIES,
                overflow: RateLimitOverflow::FoldAnonymous,
            }),
            "an unset burst must default to the rps value"
        );
        set("RATE_LIMIT_BURST", "25");
        assert_eq!(
            resolve_server_policy(&config)
                .expect("a positive burst should resolve")
                .rate_limit,
            Some(RateLimitPolicy {
                rps: 10,
                burst: 25,
                trusted_identity_header: None,
                max_identities: policy::DEFAULT_RATE_LIMIT_MAX_IDENTITIES,
                overflow: RateLimitOverflow::FoldAnonymous,
            })
        );
        // Identity capacity and overflow default to the bounded fold policy.
        unset("RATE_LIMIT_MAX_IDENTITIES");
        unset("RATE_LIMIT_OVERFLOW");
        let resolved = resolve_server_policy(&config).expect("defaults should resolve");
        let limited = resolved.rate_limit.expect("rate limit should resolve");
        assert_eq!(
            limited.max_identities,
            policy::DEFAULT_RATE_LIMIT_MAX_IDENTITIES
        );
        assert_eq!(limited.overflow, RateLimitOverflow::FoldAnonymous);
        // Out-of-range budgets and unknown overflow modes refuse boot.
        for bad in [
            "0",
            "fast",
            "",
            "-1",
            "1.5",
            "65537",
            "99999999999999999999",
        ] {
            set("RATE_LIMIT_MAX_IDENTITIES", bad);
            let error = resolve_server_policy(&config)
                .expect_err("invalid identity budget must refuse boot");
            assert!(
                error.contains("RATE_LIMIT_MAX_IDENTITIES"),
                "refusal must name the budget: {error}"
            );
        }
        unset("RATE_LIMIT_MAX_IDENTITIES");
        set("RATE_LIMIT_OVERFLOW", "evict");
        let error = resolve_server_policy(&config).expect_err("unknown overflow must refuse boot");
        assert!(error.contains("RATE_LIMIT_OVERFLOW"), "{error}");
        // A nondefault budget with explicit reject resolves.
        set("RATE_LIMIT_MAX_IDENTITIES", "2");
        set("RATE_LIMIT_OVERFLOW", "reject");
        let resolved = resolve_server_policy(&config).expect("valid capacity knobs should resolve");
        let limited = resolved.rate_limit.expect("rate limit should resolve");
        assert_eq!(limited.max_identities, 2);
        assert_eq!(limited.overflow, RateLimitOverflow::RejectNew);
        unset("RATE_LIMIT_RPS");
        unset("RATE_LIMIT_BURST");
        unset("RATE_LIMIT_MAX_IDENTITIES");
        unset("RATE_LIMIT_OVERFLOW");
    }

    #[tokio::test]
    async fn metrics_cardinality_env_knobs_are_resolved_strictly() {
        if crate::tests::isolate_environment_test(
            "server::serve::tests::metrics_cardinality_env_knobs_are_resolved_strictly",
        ) {
            return;
        }
        // E04: metrics cardinality knobs are fail-closed. Zero, garbage,
        // out-of-range, and malformed pin lists refuse boot; valid values
        // freeze into the resolved policy with deterministic pin order.
        let _env_guard = BOOT_ENV_GUARD.lock().await;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e04-metrics-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let config = boot_config(&generators, &runs);
        let unset = |variable: &str| {
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe {
                std::env::remove_var(variable);
            }
        };
        let set = |variable: &str, value: &str| {
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe {
                std::env::set_var(variable, value);
            }
        };
        unset("METRICS_GENERATOR_LIMIT");
        unset("METRICS_PINNED_GENERATORS");
        let resolved = resolve_server_policy(&config).expect("unset metrics knobs should resolve");
        assert_eq!(
            resolved.metrics_policy.generator_limit,
            policy::DEFAULT_METRICS_GENERATOR_LIMIT
        );
        assert!(resolved.metrics_policy.pinned_generators.is_empty());
        for bad in ["0", "fast", "", "-1", "1.5", "257", "99999999999999999999"] {
            set("METRICS_GENERATOR_LIMIT", bad);
            let error = resolve_server_policy(&config)
                .expect_err("invalid metrics budget must refuse boot");
            assert!(
                error.contains("METRICS_GENERATOR_LIMIT"),
                "refusal must name the budget: {error}"
            );
        }
        unset("METRICS_GENERATOR_LIMIT");
        for bad in ["a,,b", ",", "a, ,b", " "] {
            // " " trims to empty and means unset, so it resolves; the rest
            // must refuse. Handle the whitespace-only case explicitly.
            if bad.trim().is_empty() {
                set("METRICS_PINNED_GENERATORS", bad);
                assert!(
                    resolve_server_policy(&config)
                        .expect("whitespace-only pins should resolve empty")
                        .metrics_policy
                        .pinned_generators
                        .is_empty()
                );
                continue;
            }
            set("METRICS_PINNED_GENERATORS", bad);
            let error =
                resolve_server_policy(&config).expect_err("malformed pin list must refuse boot");
            assert!(
                error.contains("METRICS_PINNED_GENERATORS"),
                "refusal must name the pin list: {error}"
            );
        }
        // Too many pins refuse boot.
        let many = (0..65)
            .map(|i| format!("gen-{i}"))
            .collect::<Vec<_>>()
            .join(",");
        set("METRICS_PINNED_GENERATORS", &many);
        let error =
            resolve_server_policy(&config).expect_err("oversized pin list must refuse boot");
        assert!(error.contains("METRICS_PINNED_GENERATORS"), "{error}");
        // A nondefault budget with pins resolves and pins sort
        // deterministically.
        set("METRICS_GENERATOR_LIMIT", "2");
        set("METRICS_PINNED_GENERATORS", "b,a,b");
        let resolved = resolve_server_policy(&config).expect("valid metrics knobs should resolve");
        assert_eq!(resolved.metrics_policy.generator_limit, 2);
        assert_eq!(
            resolved.metrics_policy.pinned_generators,
            vec!["a".to_string(), "b".to_string()]
        );
        unset("METRICS_GENERATOR_LIMIT");
        unset("METRICS_PINNED_GENERATORS");
    }

    #[test]
    fn bounded_policy_envs_refuse_out_of_range_values() {
        if crate::tests::isolate_environment_test(
            "server::serve::tests::bounded_policy_envs_refuse_out_of_range_values",
        ) {
            return;
        }
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("env-bounds-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let config = boot_config(&generators, &runs);
        let unset = |variable: &str| {
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe { std::env::remove_var(variable) };
        };
        let set = |variable: &str, value: &str| {
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe { std::env::set_var(variable, value) };
        };
        // Defaults resolve to the documented values.
        for variable in [
            "READ_CACHE_MAX_BYTES",
            "LIVE_EVENT_CHANNEL_CAPACITY",
            "JOURNAL_POLL_INTERVAL_MS",
            "MAX_DIRECTORY_SCAN_ENTRIES",
            "SHARED_RESCAN_SECS",
            "QUEUED_RESUMER_SECS",
            "SHUTDOWN_DRAIN_SECS",
            "SHUTDOWN_SETTLE_SECS",
        ] {
            unset(variable);
        }
        let policy = resolve_server_policy(&config).expect("defaults should resolve");
        assert_eq!(
            policy.live_event_channel_capacity,
            policy::LIVE_EVENT_CHANNEL_CAPACITY
        );
        assert_eq!(
            policy.journal_poll_interval_millis,
            policy::JOURNAL_POLL_INTERVAL_MILLIS
        );
        assert_eq!(policy.shutdown_drain.as_secs(), 30);
        assert_eq!(policy.shutdown_settle.as_secs(), 150);

        unset("READ_CACHE_MAX_BYTES");
        assert_eq!(
            resolve_server_policy(&config).unwrap().read_cache_max_bytes,
            policy::DEFAULT_READ_CACHE_BYTES
        );
        set("READ_CACHE_MAX_BYTES", "0");
        assert!(
            resolve_server_policy(&config)
                .unwrap_err()
                .contains("READ_CACHE_MAX_BYTES")
        );
        set("READ_CACHE_MAX_BYTES", "268435456");
        assert_eq!(
            resolve_server_policy(&config).unwrap().read_cache_max_bytes,
            256 * 1024 * 1024
        );
        unset("READ_CACHE_MAX_BYTES");

        // Out-of-range values refuse boot with the variable named.
        set("LIVE_EVENT_CHANNEL_CAPACITY", "1");
        let error = resolve_server_policy(&config).expect_err("below-minimum capacity must refuse");
        assert!(error.contains("LIVE_EVENT_CHANNEL_CAPACITY"), "{error}");
        unset("LIVE_EVENT_CHANNEL_CAPACITY");
        set("JOURNAL_POLL_INTERVAL_MS", "5001");
        let error = resolve_server_policy(&config).expect_err("above-maximum cadence must refuse");
        assert!(error.contains("JOURNAL_POLL_INTERVAL_MS"), "{error}");
        unset("JOURNAL_POLL_INTERVAL_MS");

        // A settle deadline shorter than the drain refuses boot.
        set("SHUTDOWN_DRAIN_SECS", "60");
        set("SHUTDOWN_SETTLE_SECS", "30");
        let error = resolve_server_policy(&config).expect_err("settle < drain must refuse");
        assert!(error.contains("SHUTDOWN_SETTLE_SECS"), "{error}");
        // A valid explicit pair resolves.
        set("SHUTDOWN_SETTLE_SECS", "120");
        let policy = resolve_server_policy(&config).expect("valid shutdown phases should resolve");
        assert_eq!(policy.shutdown_drain.as_secs(), 60);
        assert_eq!(policy.shutdown_settle.as_secs(), 120);
        unset("SHUTDOWN_DRAIN_SECS");
        unset("SHUTDOWN_SETTLE_SECS");
    }

    #[tokio::test]
    async fn rate_limit_answers_429_only_inside_the_shutdown_gate() {
        // The limiter is layered inside the shutdown gate: outside drain the
        // second request over the burst is answered with 429 and an integer
        // Retry-After, while a draining server still answers 503 before the
        // limiter can answer 429 (E05).
        use tower::ServiceExt as _;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("rate-limit-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let shutdown = CancellationToken::new();
        let service = single_run_service(&generators, &runs);
        let state = Arc::new(test_state(service, runs.clone(), None, shutdown.clone()));
        let config = boot_config(&generators, &runs);
        let validated_cors =
            parse_cors_origins(&config.cors_origins).expect("test CORS should parse");
        let policy = RateLimitPolicy {
            rps: 1,
            burst: 1,
            trusted_identity_header: None,
            max_identities: policy::DEFAULT_RATE_LIMIT_MAX_IDENTITIES,
            overflow: RateLimitOverflow::FoldAnonymous,
        };
        let app = build_router(&state, &config, &validated_cors, Some(policy))
            .expect("router should build");
        let read = || {
            axum::http::Request::builder()
                .uri("/api/openapi.json")
                .body(axum::body::Body::empty())
                .expect("request should build")
        };
        let response = app.clone().oneshot(read()).await.expect("first read");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let response = app.clone().oneshot(read()).await.expect("second read");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "the second read must exceed the one-token burst"
        );
        assert!(
            response
                .headers()
                .contains_key(axum::http::header::RETRY_AFTER),
            "the 429 must carry Retry-After"
        );
        shutdown.cancel();
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/runs")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from("{}"))
                    .expect("request should build"),
            )
            .await
            .expect("draining request should respond");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "draining must answer 503 before the limiter can answer 429"
        );
    }

    #[tokio::test]
    async fn invalid_boot_policy_refuses_serve_with_listener() {
        if crate::tests::isolate_environment_test(
            "server::serve::tests::invalid_boot_policy_refuses_serve_with_listener",
        ) {
            return;
        }
        // E04: every deployment knob refuses boot before the run-store lock
        // or recovery, with zero side effects and a released lock.
        // Covers env knobs, total-step ceiling, and capacity bounds. Provider
        // path and lock contention fail later at service construction, still
        // before router/resume/resident start (see
        // service_construction_failure_starts_no_resident_tasks).
        let _env_guard = BOOT_ENV_GUARD.lock().await;
        let cases: Vec<(&str, &str, &str)> = vec![
            ("AUTO_GC", "maybe", "GC"),
            ("PREEMPTION", "maybe", "preemption"),
            ("RATE_LIMIT_RPS", "0", "rate limit"),
            ("RATE_LIMIT_RPS", "fast", "rate limit"),
            ("IDEMPOTENCY_TTL_SECS", "0", "idempotency"),
            ("IDEMPOTENCY_TTL_SECS", "soon", "idempotency"),
            ("IDEMPOTENCY_MAX_ENTRIES", "0", "idempotency"),
            ("IDEMPOTENCY_MAX_ENTRIES", "many", "idempotency"),
        ];
        for (variable, value, part) in cases {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .expect("temporary directory path should be UTF-8")
                .join(format!("e04-boot-{}", uuid::Uuid::now_v7()));
            let _temp_guard = TempGuard(root.clone());
            let generators = root.join("generators");
            let runs = root.join("runs");
            std::fs::create_dir_all(&generators).expect("generators dir should create");
            // SAFETY: the guard serializes environment mutation across tests.
            unsafe {
                std::env::set_var(variable, value);
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let error = serve_with_listener(boot_config(&generators, &runs), listener)
                .await
                .expect_err(&format!("invalid {variable} must refuse boot"));
            assert!(
                error.to_string().contains(part),
                "refusal must name its cause for {variable}: {error}"
            );
            // SAFETY: still holding the guard.
            unsafe {
                std::env::remove_var(variable);
            }
            // A refused boot must not pin the run-store lock.
            try_single_run_service(&generators, &runs)
                .expect("the run-store lock must be released after a refused boot");
        }
        // Inconsistent run limits refuse through the same serve path with
        // zero side effects and a released lock (E04).
        for (active, tracked) in [(0usize, 10usize), (4usize, 2usize)] {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .expect("temporary directory path should be UTF-8")
                .join(format!("e04-limits-{}", uuid::Uuid::now_v7()));
            let _temp_guard = TempGuard(root.clone());
            let generators = root.join("generators");
            let runs = root.join("runs");
            std::fs::create_dir_all(&generators).expect("generators dir should create");
            let mut config = boot_config(&generators, &runs);
            config.max_active_runs = active;
            config.max_tracked_runs = tracked;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let error = serve_with_listener(config, listener)
                .await
                .expect_err("inconsistent run limits must refuse boot");
            assert!(
                error.to_string().contains("max_active_runs")
                    || error.to_string().contains("max_tracked_runs"),
                "limit refusal must name its cause: {error}"
            );
            try_single_run_service(&generators, &runs)
                .expect("the run-store lock must be released after a refused boot");
        }
        // Invalid CORS origins refuse through the same serve path.
        {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .expect("temporary directory path should be UTF-8")
                .join(format!("e04-boot-cors-{}", uuid::Uuid::now_v7()));
            let _temp_guard = TempGuard(root.clone());
            let generators = root.join("generators");
            let runs = root.join("runs");
            std::fs::create_dir_all(&generators).expect("generators dir should create");
            let mut config = boot_config(&generators, &runs);
            config.cors_origins = vec!["not a valid origin \n".to_string()];
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let error = serve_with_listener(config, listener)
                .await
                .expect_err("invalid CORS origins must refuse boot");
            assert!(
                error.to_string().contains("CORS") || error.to_string().contains("origin"),
                "CORS refusal must name its cause: {error}"
            );
            try_single_run_service(&generators, &runs)
                .expect("the run-store lock must be released after a refused boot");
        }
    }

    #[tokio::test]
    async fn service_construction_failure_starts_no_resident_tasks() {
        // E04: provider path and lock contention fail at service
        // construction, still before router, resume, and resident start.
        // No marker journal or resident task may exist afterwards.
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e04-construction-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        // Bad provider path fails service construction.
        let mut bad_provider = boot_config(&generators, &runs);
        bad_provider.providers_path = Some(root.join("missing-providers.toml"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        serve_with_listener(bad_provider, listener)
            .await
            .expect_err("bad provider path must refuse boot");
        assert!(
            !runs.join("idempotency").exists(),
            "failed construction must not leave idempotency state"
        );
        // Lock contention fails the same way with no resident start. The
        // refused boot above must release the runs directory first.
        let _holder = acquire_store_after_release(&generators, &runs).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        serve_with_listener(boot_config(&generators, &runs), listener)
            .await
            .expect_err("contended lock must refuse boot");
    }

    #[tokio::test]
    async fn every_server_config_knob_has_an_explicit_fail_closed_bound() {
        // E04: every `ServerConfig` field refuses boot with an explicit
        // bound instead of degrading silently. One case per knob, all
        // through the single `resolve_server_policy` gate.
        let _env_guard = BOOT_ENV_GUARD.lock().await;
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("e04-knobs-{}", uuid::Uuid::now_v7())),
        )
        .expect("temp path must be UTF-8");
        let generators = root.join("generators");
        let runs = root.join("runs");
        let mut config = boot_config(&generators, &runs);
        // Valid baseline resolves.
        resolve_server_policy(&config).expect("baseline config should resolve");
        // Zero explicit-max knobs refuse. Named alias keeps the
        // `Vec<fn + str>` shape below the type-complexity lint (E04).
        type KnobCase = (fn(&mut ServerConfig), &'static str);
        let zero_cases: Vec<KnobCase> = vec![
            (
                |config: &mut ServerConfig| config.max_request_bytes = Some(0),
                "max_request_bytes",
            ),
            (
                |config: &mut ServerConfig| config.max_artifact_bytes = Some(0),
                "max_artifact_bytes",
            ),
            (
                (|config: &mut ServerConfig| config.max_artifact_entries = Some(0))
                    as fn(&mut ServerConfig),
                "max_artifact_entries",
            ),
            (
                |config: &mut ServerConfig| config.max_asset_bytes = Some(0),
                "max_asset_bytes",
            ),
            (
                |config: &mut ServerConfig| config.max_total_steps = Some(0),
                "max_total_steps",
            ),
            (
                |config: &mut ServerConfig| config.max_active_runs = 0,
                "max_active_runs",
            ),
            (
                |config: &mut ServerConfig| config.max_tracked_runs = 0,
                "max_tracked_runs",
            ),
        ];
        for (set, name) in zero_cases {
            let mut bad = config.clone();
            set(&mut bad);
            let error = resolve_server_policy(&bad).expect_err(&format!("{name} must refuse boot"));
            assert!(error.contains(name), "refusal must name {name}: {error}");
        }
        // Upper bounds refuse.
        {
            let mut bad = config.clone();
            bad.max_active_runs = 65_537;
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("oversized max_active_runs must refuse")
                    .contains("max_active_runs")
            );
        }
        {
            let mut bad = config.clone();
            bad.max_active_runs = 4;
            bad.max_tracked_runs = 2;
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("inconsistent limits must refuse")
                    .contains("max_tracked_runs")
            );
        }
        {
            let mut bad = config.clone();
            bad.max_tracked_runs = 1_000_001;
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("oversized max_tracked_runs must refuse")
                    .contains("max_tracked_runs")
            );
        }
        // Directory knobs refuse empty paths (would silently resolve to cwd).
        {
            let mut bad = config.clone();
            bad.generators_dir = camino::Utf8PathBuf::new();
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("empty generators_dir must refuse")
                    .contains("generators_dir")
            );
        }
        {
            let mut bad = config.clone();
            bad.runs_dir = camino::Utf8PathBuf::new();
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("empty runs_dir must refuse")
                    .contains("runs_dir")
            );
        }
        {
            let mut bad = config.clone();
            bad.extra_generators_dirs = vec![camino::Utf8PathBuf::new()];
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("empty extra dir must refuse")
                    .contains("extra_generators_dirs")
            );
        }
        {
            let mut bad = config.clone();
            bad.providers_path = Some(camino::Utf8PathBuf::new());
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("empty providers_path must refuse")
                    .contains("providers_path")
            );
        }
        {
            let mut bad = config.clone();
            bad.api_token = Some(String::new());
            assert!(
                resolve_server_policy(&bad)
                    .expect_err("empty api_token must refuse")
                    .contains("api_token")
            );
        }
        // Non-zero explicit maxima resolve (None stays unlimited).
        config.max_request_bytes = Some(1024);
        config.max_artifact_bytes = Some(1024);
        config.max_artifact_entries = Some(10);
        config.max_asset_bytes = Some(1024);
        config.api_token = Some("secret".into());
        resolve_server_policy(&config).expect("non-zero maxima should resolve");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn resolved_policy_freeze_survives_mid_boot_env_change() {
        if crate::tests::isolate_environment_test(
            "server::serve::tests::resolved_policy_freeze_survives_mid_boot_env_change",
        ) {
            return;
        }
        // E04 single freeze: the main path resolves once before bind and
        // serves with the frozen policy. A mid-boot environment change must
        // not move the served values: the resolved path proceeds past policy
        // (failing later at service construction for the bad providers path),
        // while a fresh resolve refuses with the env error.
        let _env_guard = BOOT_ENV_GUARD.lock().await;
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e04-freeze-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let mut config = boot_config(&generators, &runs);
        config.providers_path = Some(root.join("missing-providers.toml"));
        let policy = resolve_server_policy(&config).expect("policy should resolve clean");
        // Freeze proves equality: two resolves under one env agree.
        let again = resolve_server_policy(&boot_config(&generators, &runs))
            .expect("second resolve should agree");
        assert_eq!(
            policy.idempotency_ttl, again.idempotency_ttl,
            "single freeze must keep TTL identical"
        );
        assert_eq!(policy, again, "frozen policy must equal a fresh resolve");
        // Mid-boot env change: fresh resolve refuses, frozen policy does not.
        // SAFETY: the guard serializes environment mutation across tests.
        unsafe {
            std::env::set_var("AUTO_GC", "maybe");
        }
        assert!(
            resolve_server_policy(&config).is_err(),
            "fresh resolve with poisoned env must refuse"
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let error = serve_with_resolved_policy_and_deadline(
            policy,
            config,
            listener,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect_err("frozen policy must survive the env change past policy");
        assert!(
            !error.to_string().contains("GC"),
            "frozen serve must not fail on the poisoned env: {error}"
        );
        // SAFETY: still holding the guard.
        unsafe {
            std::env::remove_var("AUTO_GC");
        }
    }

    #[tokio::test]
    async fn router_failure_starts_zero_recovery_and_releases_the_lock() {
        // E04: service construction (lock + directories) happens before
        // router build by design, but no recovery or resident task starts
        // until the router succeeds. A router refusal (here: a real zero
        // explicit-max that the router re-checks defense-in-depth
        // post-service) must start zero recovery, release the lock, and
        // journal no markers. Directories are intentionally left (they may
        // hold foreign runs); only the lock is released.
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path should be UTF-8")
            .join(format!("e04-router-fail-{}", uuid::Uuid::now_v7()));
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generators).expect("generators dir should create");
        let service = try_single_run_service(&generators, &runs)
            .expect("service should initialize and hold the lock");
        let state = Arc::new(test_state(
            service,
            runs.clone(),
            None,
            CancellationToken::new(),
        ));
        let mut bad = boot_config(&generators, &runs);
        bad.max_request_bytes = Some(0);
        let validated: Vec<HeaderValue> = Vec::new();
        build_router(&state, &bad, &validated, None)
            .expect_err("zero max_request_bytes must refuse the router post-service");
        // Zero recovery: no idempotency state, no run journals, no markers.
        assert!(
            !runs.join("idempotency").exists(),
            "a refused router must start zero recovery"
        );
        // Construction side effects are explicit (E04): the runs directory
        // and the `.service.lock` file exist even on refusal — they may hold
        // foreign runs, so refusal never removes them. Only the lock fd is
        // released by drop, which the rebuild below proves.
        assert!(
            runs.exists(),
            "service construction creates the runs dir even on refused router"
        );
        assert!(
            runs.join(".service.lock").exists(),
            "the lock file remains on refusal; only the fd is released"
        );
        let journals: Vec<_> = std::fs::read_dir(&runs)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| {
                        entry.file_name().to_string_lossy().ends_with(".jsonl")
                            || entry.file_name().to_string_lossy().contains("marker")
                    })
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            journals.is_empty(),
            "a refused router must journal no markers: {journals:?}"
        );
        // Lock released: dropping the refused state frees the store.
        drop(state);
        acquire_store_after_release(&generators, &runs).await;
    }
}
