//! Managed-instance lifecycle shared by every caller: provision an
//! isolated instance, enter it for one-shot or long-lived workloads, and
//! always clean it up. Docker-compatible backends stay one-shot (`run`);
//! Incus-like and legacy LXC backends follow init/configure/start/exec/
//! stop/delete with a guard that cleans up even when the awaiting future is
//! dropped by an outer timeout or abort.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::backend::Backend;
use super::error::ContainerError;
use super::plans::{self, Mount};

/// Bounds for daemon operations. Image downloads on first create dominate,
/// so creation gets the largest budget; every bound surfaces explicitly
/// instead of hanging.
pub const CREATE_TIMEOUT: Duration = Duration::from_secs(600);
pub const START_TIMEOUT: Duration = Duration::from_secs(120);
pub const ADMIN_TIMEOUT: Duration = Duration::from_secs(60);
pub const STOP_TIMEOUT: Duration = Duration::from_secs(30);
pub const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// How a live instance is addressed during cleanup.
#[derive(Debug, Clone)]
pub enum InstanceId {
    /// Docker-family tracking file holding the container id.
    CidFile(PathBuf),
    /// Managed instance name (Incus-like, legacy LXC).
    Name(String),
}

/// A live container handle owned until [`teardown`] runs.
#[derive(Debug, Clone)]
pub struct Session {
    pub backend: Backend,
    pub id: InstanceId,
}

/// Guard that cleans up unless disarmed after an awaited teardown
/// succeeded. Drop is the last-resort backstop only: every normal path
/// awaits [`teardown`] and disarms solely on success, so a failed awaited
/// teardown stays armed and the Drop backstop retries it through
/// [`teardown_sync`]. Detached Drop threads never block an async executor;
/// shutdown joins them via [`await_outstanding_cleanups`].
pub struct SessionGuard {
    session: Option<Session>,
}

impl SessionGuard {
    pub fn new(session: Session) -> Self {
        Self {
            session: Some(session),
        }
    }

    pub fn disarm(mut self) {
        self.session = None;
    }
}

/// Detached Drop-thread cleanups still in flight. Shutdown paths await
/// this counter so "stopped" means external work actually stopped instead
/// of racing detached teardown threads.
static OUTSTANDING_CLEANUPS: AtomicUsize = AtomicUsize::new(0);

/// Waits until Drop-spawned cleanups finish, up to `timeout`. Returns the
/// number of cleanups still outstanding so shutdown reports unrecovered
/// instances instead of claiming a clean stop. Logs the remainder instead
/// of wedging shutdown forever.
pub async fn await_outstanding_cleanups(timeout: Duration) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = OUTSTANDING_CLEANUPS.load(Ordering::Acquire);
        if remaining == 0 {
            return 0;
        }
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                remaining,
                "container cleanups still outstanding past shutdown deadline"
            );
            return remaining;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Runs a blocking bounded teardown off the dropping thread and tracks it
/// for shutdown joining. Failures are logged with the instance identity;
/// an unspawnable thread falls back to inline teardown because blocking is
/// preferable to orphaning the instance.
fn spawn_detached_cleanup(session: Session, config_path: Option<PathBuf>) {
    OUTSTANDING_CLEANUPS.fetch_add(1, Ordering::AcqRel);
    let thread_session = session.clone();
    let thread_config = config_path.clone();
    let spawned = std::thread::Builder::new()
        .name("qcg-container-teardown".into())
        .spawn(move || {
            if let Err(error) = teardown_sync(&thread_session) {
                tracing::warn!(%error, "detached container teardown failed");
            }
            if let Some(path) = &thread_config {
                let _ = std::fs::remove_file(path);
            }
            OUTSTANDING_CLEANUPS.fetch_sub(1, Ordering::AcqRel);
        })
        .is_ok();
    if !spawned {
        OUTSTANDING_CLEANUPS.fetch_sub(1, Ordering::AcqRel);
        if let Err(error) = teardown_sync(&session) {
            tracing::warn!(%error, "inline container teardown failed");
        }
        if let Some(path) = config_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        // Never block the dropping thread: this guard routinely dies on
        // async executor threads, where a bounded teardown would stall
        // unrelated work.
        spawn_detached_cleanup(session, None);
    }
}

/// Ownership of an in-progress provision, held from before the first daemon
/// side effect. If the provisioning future is dropped (outer timeout or
/// abort) or errors, the guard cleans the partially created instance by
/// name; `commit` transfers ownership to the live [`Session`] on success.
/// Teardown commands are idempotent, so cleaning a never-created instance
/// is a harmless no-op.
struct ProvisionGuard {
    backend: Backend,
    name: String,
    config_path: Option<PathBuf>,
    committed: bool,
}

impl ProvisionGuard {
    fn new(backend: &Backend, name: String, config_path: Option<PathBuf>) -> Self {
        Self {
            backend: backend.clone(),
            name,
            config_path,
            committed: false,
        }
    }

    /// Transfers ownership to the live session; suppresses Drop cleanup.
    fn commit(mut self) {
        self.committed = true;
    }

    /// Releases ownership after an explicitly awaited cleanup succeeded;
    /// suppresses the Drop backstop so a settled provision is not torn
    /// down twice.
    fn disarm(mut self) {
        self.committed = true;
    }
}

impl Drop for ProvisionGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let session = Session {
            backend: self.backend.clone(),
            id: InstanceId::Name(self.name.clone()),
        };
        spawn_detached_cleanup(session, self.config_path.clone());
    }
}

pub struct Provision<'a> {
    pub image: &'a str,
    pub mounts: &'a [Mount<'a>],
    /// Filename prefix for docker-family tracking files in the temp dir.
    pub id_prefix: &'a str,
    pub cancel: &'a CancellationToken,
}

/// Provisions an isolated instance. Docker-family backends only allocate a
/// tracking file; managed backends create, configure, and start the
/// instance and wait for readiness. Failures clean up partial state before
/// returning so no orphan survives a failed provision.
pub async fn provision(backend: &Backend, spec: &Provision<'_>) -> Result<Session, ContainerError> {
    plans::validate_image_for_backend(backend, spec.image)?;
    match backend {
        Backend::Docker { .. } => {
            let cidfile = std::env::temp_dir().join(format!(
                ".{}-{}.cid",
                spec.id_prefix,
                uuid::Uuid::now_v7().as_simple()
            ));
            Ok(Session {
                backend: backend.clone(),
                id: InstanceId::CidFile(cidfile),
            })
        }
        Backend::Incus { binary } => provision_incus(binary, spec).await,
        Backend::Lxc => provision_lxc(spec).await,
    }
}

async fn provision_incus(binary: &str, spec: &Provision<'_>) -> Result<Session, ContainerError> {
    let name = plans::instance_name();
    // Own the instance name before the first daemon side effect so an
    // aborted or dropped provision still cleans up (B06).
    let guard = ProvisionGuard::new(
        &Backend::Incus {
            binary: binary.to_string(),
        },
        name.clone(),
        None,
    );
    let cleanup = || Session {
        backend: Backend::Incus {
            binary: binary.to_string(),
        },
        id: InstanceId::Name(name.clone()),
    };
    let result: Result<(), ContainerError> = async {
        // Storage pool from the daemon's own default profile.
        let pool_output = run_admin(
            &plans::incus_pool_argv(binary),
            ADMIN_TIMEOUT,
            spec.cancel,
            &name,
            "discover-pool",
        )
        .await?;
        let pool = pool_output.stdout.trim().to_string();
        if pool.is_empty() {
            return Err(ContainerError::StageFailed {
                stage: "discover-pool",
                instance: name.clone(),
                detail:
                    "default profile has no root disk pool; configure one with `incus admin init`"
                        .into(),
            });
        }
        let (_, fingerprint) =
            plans::split_pinned_image(spec.image).ok_or_else(|| ContainerError::InvalidImage {
                image: spec.image.to_string(),
                backend: "incus".into(),
                reason: "image must have form `<remote>:<path>@sha256:<fingerprint>`".into(),
            })?;
        // Launch by fingerprint so exactly the pinned bits run; a missing
        // local image fails closed and tells the operator to pre-pull it.
        run_admin(
            &plans::incus_init_argv(binary, &pool, fingerprint, &name),
            CREATE_TIMEOUT,
            spec.cancel,
            &name,
            "init",
        )
        .await?;
        for (index, mount) in spec.mounts.iter().enumerate() {
            plans::validate_guest_path(mount.guest).map_err(|error| {
                ContainerError::StageFailed {
                    stage: "configure-mount",
                    instance: name.clone(),
                    detail: error.to_string(),
                }
            })?;
            run_admin(
                &plans::incus_device_add_argv(
                    binary,
                    &name,
                    &format!("qcgwork{index}"),
                    mount.host,
                    mount.guest,
                    mount.readonly,
                ),
                ADMIN_TIMEOUT,
                spec.cancel,
                &name,
                "configure-mount",
            )
            .await?;
        }
        run_admin(
            &plans::incus_secure_argv(binary, &name),
            ADMIN_TIMEOUT,
            spec.cancel,
            &name,
            "configure-security",
        )
        .await?;
        run_admin(
            &plans::incus_start_argv(binary, &name),
            START_TIMEOUT,
            spec.cancel,
            &name,
            "start",
        )
        .await?;
        wait_ready(
            &plans::incus_probe_argv(binary, &name),
            START_TIMEOUT,
            spec.cancel,
            &name,
        )
        .await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            let session = cleanup();
            guard.commit();
            Ok(session)
        }
        Err(error) => {
            // Awaited cleanup first for observability; the guard backstop
            // stays armed only if this fails, so a settled provision is
            // never torn down twice.
            if teardown(&cleanup()).await.is_err() {
                tracing::warn!(
                    instance = name.as_str(),
                    "provision cleanup failed; guard backstop remains armed"
                );
            } else {
                guard.disarm();
            }
            Err(error)
        }
    }
}

async fn provision_lxc(spec: &Provision<'_>) -> Result<Session, ContainerError> {
    let name = plans::instance_name();
    let (dist, release) =
        plans::parse_lxc_image(spec.image).ok_or_else(|| ContainerError::InvalidImage {
            image: spec.image.to_string(),
            backend: "lxc".into(),
            reason: "image must have form `<dist>:<release>` (for example `alpine:3.20`)".into(),
        })?;
    let arch = plans::map_lxc_arch(std::env::consts::ARCH).ok_or_else(|| {
        ContainerError::InvalidImage {
            image: spec.image.to_string(),
            backend: "lxc".into(),
            reason: format!(
                "host architecture `{}` has no LXC download mapping",
                std::env::consts::ARCH
            ),
        }
    })?;
    let config_path = std::env::temp_dir().join(format!("{name}.conf"));
    // Own the instance (and its config file) before the first daemon side
    // effect so an aborted or dropped provision still cleans up (B06).
    let guard = ProvisionGuard::new(&Backend::Lxc, name.clone(), Some(config_path.clone()));
    let config_text =
        plans::lxc_config_text(spec.mounts).map_err(|error| ContainerError::StageFailed {
            stage: "configure",
            instance: name.clone(),
            detail: error.to_string(),
        })?;
    // create_new refuses to overwrite an unrelated file at the temp path.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&config_path)?
        .write_all(config_text.as_bytes())?;
    let session = Session {
        backend: Backend::Lxc,
        id: InstanceId::Name(name.clone()),
    };
    let result: Result<(), ContainerError> = async {
        run_admin(
            &plans::lxc_create_argv(&name, &config_path, &dist, &release, arch),
            CREATE_TIMEOUT,
            spec.cancel,
            &name,
            "create",
        )
        .await?;
        run_admin(
            &plans::lxc_start_argv(&name),
            START_TIMEOUT,
            spec.cancel,
            &name,
            "start",
        )
        .await?;
        wait_ready(
            &plans::lxc_probe_argv(&name),
            START_TIMEOUT,
            spec.cancel,
            &name,
        )
        .await?;
        Ok(())
    }
    .await;
    let _ = std::fs::remove_file(&config_path);
    match result {
        Ok(()) => {
            guard.commit();
            Ok(session)
        }
        Err(error) => {
            if let Err(cleanup_error) = teardown(&session).await {
                tracing::warn!(
                    %cleanup_error,
                    instance = name.as_str(),
                    "provision cleanup failed; guard backstop remains armed"
                );
            } else {
                guard.disarm();
            }
            Err(error)
        }
    }
}

/// Awaited teardown on every exit path. Returns infrastructure failures
/// (spawn failure, daemon timeout, unexpected non-zero exit) so callers log
/// them with the instance identity; the Drop guard re-attempts via
/// [`teardown_sync`]. Stop/kill failures never abort the subsequent
/// delete/remove: teardown is idempotent, so "already stopped" must not
/// strand the instance. A delete/remove reporting the instance absent is
/// success; any other failure keeps the tracking identity for retry.
pub async fn teardown(session: &Session) -> Result<(), ContainerError> {
    match session {
        Session {
            backend: Backend::Docker { binary, .. },
            id: InstanceId::CidFile(cidfile),
        } => {
            let id = std::fs::read_to_string(cidfile)
                .map(|id| id.trim().to_string())
                .unwrap_or_default();
            if id.is_empty() {
                let _ = std::fs::remove_file(cidfile);
                return Ok(());
            }
            if let Err(error) =
                run_admin_timed(&[binary.as_str(), "kill", &id], STOP_TIMEOUT, &id, "kill").await
            {
                tracing::warn!(%error, instance = id.as_str(), "container kill failed; continuing to rm");
            }
            match run_admin_timed(&[binary.as_str(), "rm", "-f", &id], STOP_TIMEOUT, &id, "rm")
                .await
            {
                Ok(()) => {}
                Err(error) if is_absent_detail(&error.to_string()) => {
                    tracing::info!(
                        instance = id.as_str(),
                        "container already absent; treating rm as success"
                    );
                }
                Err(error) => return Err(error),
            }
            // Remove the tracking file only after successful kill+rm so a
            // failed teardown keeps the identity for operator retry.
            std::fs::remove_file(cidfile)?;
            Ok(())
        }
        Session {
            backend: Backend::Incus { binary },
            id: InstanceId::Name(name),
        } => {
            if let Err(error) = run_admin_timed_vec(
                &plans::incus_stop_argv(binary, name),
                STOP_TIMEOUT,
                name,
                "stop",
            )
            .await
            {
                if is_absent_detail(&error.to_string()) {
                    tracing::info!(
                        instance = name.as_str(),
                        "instance already absent; continuing to delete"
                    );
                } else {
                    tracing::warn!(%error, instance = name.as_str(), "instance stop failed; continuing to delete");
                }
            }
            match run_admin_timed_vec(
                &plans::incus_delete_argv(binary, name),
                STOP_TIMEOUT,
                name,
                "delete",
            )
            .await
            {
                Ok(()) => Ok(()),
                Err(error) if is_absent_detail(&error.to_string()) => {
                    tracing::info!(
                        instance = name.as_str(),
                        "instance already absent; treating delete as success"
                    );
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
        Session {
            backend: Backend::Lxc,
            id: InstanceId::Name(name),
        } => {
            if let Err(error) =
                run_admin_timed_vec(&plans::lxc_stop_argv(name), STOP_TIMEOUT, name, "stop").await
            {
                if is_absent_detail(&error.to_string()) {
                    tracing::info!(
                        instance = name.as_str(),
                        "instance already absent; continuing to destroy"
                    );
                } else {
                    tracing::warn!(%error, instance = name.as_str(), "instance stop failed; continuing to destroy");
                }
            }
            match run_admin_timed_vec(
                &plans::lxc_destroy_argv(name),
                STOP_TIMEOUT,
                name,
                "destroy",
            )
            .await
            {
                Ok(()) => Ok(()),
                Err(error) if is_absent_detail(&error.to_string()) => {
                    tracing::info!(
                        instance = name.as_str(),
                        "instance already absent; treating destroy as success"
                    );
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
        _ => Ok(()),
    }
}

/// Daemon "nothing to tear down" reports share no exit code, so match the
/// human-readable detail instead. Callers only pass daemon stderr tails
/// here, never unrelated error text.
fn is_absent_detail(detail: &str) -> bool {
    let lower = detail.to_lowercase();
    [
        "no such",
        "not found",
        "does not exist",
        "unknown instance",
        "could not find",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

/// Blocking teardown for Drop paths. Bounded per command so a wedged daemon
/// cannot hang process teardown. Same stop-then-delete contract as
/// [`teardown`]: stop failures never abort the delete, and an
/// already-absent instance is success.
pub fn teardown_sync(session: &Session) -> Result<(), ContainerError> {
    match session {
        Session {
            backend: Backend::Docker { binary, .. },
            id: InstanceId::CidFile(cidfile),
        } => {
            let id = std::fs::read_to_string(cidfile)
                .map(|id| id.trim().to_string())
                .unwrap_or_default();
            if id.is_empty() {
                let _ = std::fs::remove_file(cidfile);
                return Ok(());
            }
            for (args, stage) in [
                (vec!["kill", id.as_str()], "kill"),
                (vec!["rm", "-f", id.as_str()], "rm"),
            ] {
                if let Err(error) = run_sync_bounded(binary, &args, STOP_TIMEOUT, &id, stage) {
                    if stage == "rm" && is_absent_detail(&error.to_string()) {
                        tracing::info!(
                            instance = id.as_str(),
                            "container already absent; treating rm as success"
                        );
                        continue;
                    }
                    if stage == "kill" {
                        tracing::warn!(%error, instance = id.as_str(), "container kill failed; continuing to rm");
                        continue;
                    }
                    return Err(error);
                }
            }
            std::fs::remove_file(cidfile)?;
            Ok(())
        }
        Session {
            backend: Backend::Incus { binary },
            id: InstanceId::Name(name),
        } => {
            for (argv, stage) in [
                (plans::incus_stop_argv(binary, name), "stop"),
                (plans::incus_delete_argv(binary, name), "delete"),
            ] {
                let Some((bin, args)) = argv.split_first() else {
                    continue;
                };
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                if let Err(error) = run_sync_bounded(bin, &args, STOP_TIMEOUT, name, stage) {
                    if is_absent_detail(&error.to_string()) {
                        tracing::info!(
                            instance = name.as_str(),
                            stage,
                            "instance already absent; continuing teardown"
                        );
                        continue;
                    }
                    if stage == "stop" {
                        tracing::warn!(%error, instance = name.as_str(), "instance stop failed; continuing to delete");
                        continue;
                    }
                    return Err(error);
                }
            }
            Ok(())
        }
        Session {
            backend: Backend::Lxc,
            id: InstanceId::Name(name),
        } => {
            for (argv, stage) in [
                (plans::lxc_stop_argv(name), "stop"),
                (plans::lxc_destroy_argv(name), "destroy"),
            ] {
                let Some((bin, args)) = argv.split_first() else {
                    continue;
                };
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                if let Err(error) = run_sync_bounded(bin, &args, STOP_TIMEOUT, name, stage) {
                    if is_absent_detail(&error.to_string()) {
                        tracing::info!(
                            instance = name.as_str(),
                            stage,
                            "instance already absent; continuing teardown"
                        );
                        continue;
                    }
                    if stage == "stop" {
                        tracing::warn!(%error, instance = name.as_str(), "instance stop failed; continuing to destroy");
                        continue;
                    }
                    return Err(error);
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

struct AdminOutput {
    stdout: String,
}

async fn run_admin(
    argv: &[String],
    timeout: std::time::Duration,
    cancel: &CancellationToken,
    instance: &str,
    stage: &'static str,
) -> Result<AdminOutput, ContainerError> {
    let (bin, args) = argv
        .split_first()
        .ok_or_else(|| ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: "admin command is empty".into(),
        })?;
    let mut command = tokio::process::Command::new(bin);
    command
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    // Daemon clients resolve sockets and config without a home directory,
    // but preserve HOME when set so existing client setups keep working.
    if let Ok(home) = std::env::var("HOME") {
        command.env("HOME", home);
    }
    command.kill_on_drop(true);
    let child = command.spawn()?;
    // Dropping the wait future kills the child through kill_on_drop, so the
    // cancel and timeout branches need no explicit kill.
    let output = tokio::select! {
        _ = cancel.cancelled() => {
            return Err(ContainerError::Canceled);
        }
        output = tokio::time::timeout(timeout, child.wait_with_output()) => {
            match output {
                Ok(Ok(output)) => output,
                Ok(Err(error)) => return Err(ContainerError::Io(error)),
                Err(_) => {
                    return Err(ContainerError::StageTimedOut { stage, instance: instance.to_string(), timeout_secs: timeout.as_secs() });
                }
            }
        }
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tail: String = stderr
            .trim()
            .chars()
            .rev()
            .take(500)
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        return Err(ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: format!("exit {}: {tail}", output.status),
        });
    }
    Ok(AdminOutput {
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
    })
}

/// Fire-and-forget timed admin command where only completion matters.
/// A non-zero exit is an infrastructure failure with the daemon stderr
/// tail attached: teardown commands are idempotent, so callers decide
/// whether "already absent" is success. Spawn failure or timeout means
/// the daemon may still hold the instance and must be reported.
async fn run_admin_timed(
    cmd: &[&str],
    timeout: std::time::Duration,
    instance: &str,
    stage: &'static str,
) -> Result<(), ContainerError> {
    let Some((bin, args)) = cmd.split_first() else {
        return Err(ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: "admin command is empty".into(),
        });
    };
    let mut command = tokio::process::Command::new(*bin);
    command
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    command.kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: format!("teardown command failed to spawn: {error}"),
        })?;
    // wait_with_output would move the child, losing the timeout kill path,
    // so take the piped stderr and drain it after the wait resolves.
    let mut stderr = child.stderr.take();
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            return Err(ContainerError::StageFailed {
                stage,
                instance: instance.to_string(),
                detail: format!("teardown wait failed: {error}"),
            });
        }
        Err(_) => {
            if let Err(error) = child.kill().await {
                tracing::warn!(%error, instance = instance.to_string(), "timed-out teardown child kill failed");
            }
            if let Err(error) = child.wait().await {
                tracing::warn!(%error, instance = instance.to_string(), "timed-out teardown child wait failed");
            }
            return Err(ContainerError::StageTimedOut {
                stage,
                instance: instance.to_string(),
                timeout_secs: timeout.as_secs(),
            });
        }
    };
    if !status.success() {
        use tokio::io::AsyncReadExt as _;
        let mut err_text = String::new();
        if let Some(mut pipe) = stderr.take()
            && let Err(error) = pipe.read_to_string(&mut err_text).await
        {
            tracing::warn!(%error, instance = instance.to_string(), "teardown stderr drain failed; detail continues without it");
        }
        return Err(ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: format!("exit {status}: {}", stderr_tail(err_text.as_bytes())),
        });
    }
    Ok(())
}

/// Last 500 chars of daemon stderr for failure details. Operates on chars,
/// never byte offsets, so non-ASCII output cannot panic the cleanup path.
fn stderr_tail(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr)
        .trim()
        .chars()
        .rev()
        .take(500)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

async fn run_admin_timed_vec(
    argv: &[String],
    timeout: std::time::Duration,
    instance: &str,
    stage: &'static str,
) -> Result<(), ContainerError> {
    let parts: Vec<&str> = argv.iter().map(String::as_str).collect();
    run_admin_timed(&parts, timeout, instance, stage).await
}

/// Waits for instance readiness by probing execution itself instead of
/// parsing daemon-specific status output.
async fn wait_ready(
    probe_argv: &[String],
    timeout: std::time::Duration,
    cancel: &CancellationToken,
    instance: &str,
) -> Result<(), ContainerError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cancel.is_cancelled() {
            return Err(ContainerError::Canceled);
        }
        let Some((bin, args)) = probe_argv.split_first() else {
            return Err(ContainerError::StageFailed {
                stage: "probe",
                instance: instance.to_string(),
                detail: "probe command is empty".into(),
            });
        };
        let mut command = tokio::process::Command::new(bin);
        command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .env_clear();
        if let Ok(path) = std::env::var("PATH") {
            command.env("PATH", path);
        }
        if let Ok(home) = std::env::var("HOME") {
            command.env("HOME", home);
        }
        command.kill_on_drop(true);
        if let Ok(mut child) = command.spawn()
            && let Ok(status) = child.wait().await
            && status.success()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ContainerError::StageTimedOut {
                stage: "wait-ready",
                instance: instance.to_string(),
                timeout_secs: timeout.as_secs(),
            });
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(ContainerError::Canceled),
            _ = tokio::time::sleep(PROBE_INTERVAL) => {}
        }
    }
}

/// Blocking bounded subprocess for Drop paths. A non-zero exit is a
/// failure with the exit code attached; callers map "already absent" to
/// success.
fn run_sync_bounded(
    binary: &str,
    args: &[&str],
    timeout: std::time::Duration,
    instance: &str,
    stage: &'static str,
) -> Result<(), ContainerError> {
    use std::io::Read as _;
    let mut command = std::process::Command::new(binary);
    command
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    let Ok(mut child) = command.spawn() else {
        return Err(ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: "teardown command failed to spawn".into(),
        });
    };
    let mut stderr = child.stderr.take();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut err_text = String::new();
                if let Some(mut pipe) = stderr.take() {
                    let _ = pipe.read_to_string(&mut err_text);
                }
                if !status.success() {
                    return Err(ContainerError::StageFailed {
                        stage,
                        instance: instance.to_string(),
                        detail: format!("exit {status}: {}", stderr_tail(err_text.as_bytes())),
                    });
                }
                return Ok(());
            }
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(ContainerError::StageTimedOut {
                        stage,
                        instance: instance.to_string(),
                        timeout_secs: timeout.as_secs(),
                    });
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(error) => {
                return Err(ContainerError::StageFailed {
                    stage,
                    instance: instance.to_string(),
                    detail: format!("teardown wait failed: {error}"),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bogus_docker_session(id_contents: &str) -> (tempfile_like::TempCid, Session) {
        tempfile_like::TempCid::create(id_contents)
    }

    mod tempfile_like {
        use super::*;

        pub(crate) struct TempCid {
            pub(crate) path: PathBuf,
        }

        impl TempCid {
            pub(crate) fn create(contents: &str) -> (TempCid, Session) {
                let path = std::env::temp_dir().join(format!(
                    ".qcg-test-cid-{}",
                    uuid::Uuid::now_v7().as_simple()
                ));
                std::fs::write(&path, contents).expect("test cidfile should be written");
                let session = Session {
                    backend: Backend::Docker {
                        binary: "definitely-not-a-qcg-binary".into(),
                        runtime_flag: None,
                    },
                    id: InstanceId::CidFile(path.clone()),
                };
                (TempCid { path }, session)
            }
        }

        impl Drop for TempCid {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }

    #[test]
    fn failed_teardown_keeps_the_tracking_file() {
        // B06: a failed teardown must keep the identity for operator retry
        // instead of deleting the evidence.
        let (_guard, session) = bogus_docker_session("deadbeef");
        let error = teardown_sync(&session).expect_err("bogus binary must fail teardown");
        assert!(
            error.to_string().contains("deadbeef") || error.to_string().contains("failed to spawn"),
            "failure must identify the instance, got: {error}"
        );
    }

    #[test]
    fn failed_teardown_keeps_the_tracking_file_on_disk() {
        let (cid, session) = bogus_docker_session("deadbeef");
        let _ = teardown_sync(&session);
        assert!(
            cid.path.exists(),
            "cidfile must survive a failed teardown for operator retry"
        );
    }

    #[test]
    fn empty_tracking_file_teardown_succeeds_and_cleans_up() {
        let (cid, session) = bogus_docker_session("   \n");
        teardown_sync(&session).expect("empty id means nothing to stop");
        assert!(
            !cid.path.exists(),
            "empty tracking file carries no identity and is removed"
        );
    }

    #[test]
    fn absent_instance_reports_are_recognized() {
        // B06: daemon "nothing to tear down" reports must map to success,
        // while unrelated failures stay failures.
        for report in [
            "Error: No such container: qcg-1",
            "Error response from daemon: No such object",
            "Instance not found",
            "error: The instance does not exist",
            "lxc-destroy: qcg-1: unknown instance",
            "could not find container abc",
        ] {
            assert!(is_absent_detail(report), "{report} must read as absent");
        }
        for report in [
            "teardown command failed to spawn",
            "permission denied",
            "exit status: 1: ",
            "daemon timed out",
        ] {
            assert!(!is_absent_detail(report), "{report} must stay a failure");
        }
    }

    #[tokio::test]
    async fn dropped_provision_guard_cleanup_is_joinable() {
        // B06: ownership held across a dropped provision still cleans up,
        // and shutdown can await the detached work.
        let guard = ProvisionGuard::new(
            &Backend::Incus {
                binary: "definitely-not-a-qcg-binary".into(),
            },
            format!("qcgtest{}", uuid::Uuid::now_v7().as_simple()),
            None,
        );
        drop(guard);
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            await_outstanding_cleanups(std::time::Duration::from_secs(25)),
        )
        .await
        .expect("detached provision cleanup must complete");
    }

    #[tokio::test]
    async fn idle_cleanup_await_returns_immediately() {
        await_outstanding_cleanups(std::time::Duration::from_secs(5)).await;
    }
}
