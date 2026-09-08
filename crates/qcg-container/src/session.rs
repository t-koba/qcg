//! Managed-instance lifecycle shared by every caller: provision an
//! isolated instance, enter it for one-shot or long-lived workloads, and
//! always clean it up. Docker-compatible backends stay one-shot (`run`);
//! Incus-like and legacy LXC backends follow init/configure/start/exec/
//! stop/delete with a guard that cleans up even when the awaiting future is
//! dropped by an outer timeout or abort.

use std::io::Write as _;
use std::path::PathBuf;
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

/// Guard that cleans up unless disarmed after awaited teardown. Drop runs
/// the blocking cleanup directly, mirroring the established transport Drop
/// discipline, so cancellation never orphans an instance.
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

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        // Never block the dropping thread: this guard routinely dies on
        // async executor threads, where a bounded 60s teardown would stall
        // unrelated work. A detached thread performs the same bounded
        // teardown instead. If the spawn itself fails (teardown-time
        // resource exhaustion), fall back to inline teardown: blocking is
        // preferable to orphaning the instance.
        let spawned = {
            let session = session.clone();
            std::thread::Builder::new()
                .name("qcg-container-teardown".into())
                .spawn(move || teardown_sync(&session))
                .is_ok()
        };
        if !spawned {
            teardown_sync(&session);
        }
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
        Ok(()) => Ok(cleanup()),
        Err(error) => {
            teardown(&cleanup()).await;
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
        Ok(()) => Ok(session),
        Err(error) => {
            teardown(&session).await;
            Err(error)
        }
    }
}

/// Awaited best-effort teardown on every exit path. Failures are returned
/// so callers log them; the Drop guard re-attempts via [`teardown_sync`].
pub async fn teardown(session: &Session) {
    match session {
        Session {
            backend: Backend::Docker { binary, .. },
            id: InstanceId::CidFile(cidfile),
        } => {
            let id = std::fs::read_to_string(cidfile)
                .map(|id| id.trim().to_string())
                .unwrap_or_default();
            let _ = std::fs::remove_file(cidfile);
            if id.is_empty() {
                return;
            }
            run_admin_timed(&[binary.as_str(), "kill", &id], STOP_TIMEOUT).await;
            run_admin_timed(&[binary.as_str(), "rm", "-f", &id], STOP_TIMEOUT).await;
        }
        Session {
            backend: Backend::Incus { binary },
            id: InstanceId::Name(name),
        } => {
            run_admin_timed_vec(&plans::incus_stop_argv(binary, name), STOP_TIMEOUT).await;
            run_admin_timed_vec(&plans::incus_delete_argv(binary, name), STOP_TIMEOUT).await;
        }
        Session {
            backend: Backend::Lxc,
            id: InstanceId::Name(name),
        } => {
            run_admin_timed_vec(&plans::lxc_stop_argv(name), STOP_TIMEOUT).await;
            run_admin_timed_vec(&plans::lxc_destroy_argv(name), STOP_TIMEOUT).await;
        }
        _ => {}
    }
}

/// Blocking teardown for Drop paths. Bounded per command so a wedged daemon
/// cannot hang process teardown.
pub fn teardown_sync(session: &Session) {
    match session {
        Session {
            backend: Backend::Docker { binary, .. },
            id: InstanceId::CidFile(cidfile),
        } => {
            let id = std::fs::read_to_string(cidfile)
                .map(|id| id.trim().to_string())
                .unwrap_or_default();
            let _ = std::fs::remove_file(cidfile);
            if id.is_empty() {
                return;
            }
            for args in [vec!["kill", id.as_str()], vec!["rm", "-f", id.as_str()]] {
                run_sync_bounded(binary, &args, STOP_TIMEOUT);
            }
        }
        Session {
            backend: Backend::Incus { binary },
            id: InstanceId::Name(name),
        } => {
            for argv in [
                plans::incus_stop_argv(binary, name),
                plans::incus_delete_argv(binary, name),
            ] {
                let Some((bin, args)) = argv.split_first() else {
                    continue;
                };
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                run_sync_bounded(bin, &args, STOP_TIMEOUT);
            }
        }
        Session {
            backend: Backend::Lxc,
            id: InstanceId::Name(name),
        } => {
            for argv in [plans::lxc_stop_argv(name), plans::lxc_destroy_argv(name)] {
                let Some((bin, args)) = argv.split_first() else {
                    continue;
                };
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                run_sync_bounded(bin, &args, STOP_TIMEOUT);
            }
        }
        _ => {}
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
                    return Err(ContainerError::StageTimedOut { stage, instance: instance.to_string() });
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
async fn run_admin_timed(cmd: &[&str], timeout: std::time::Duration) {
    let Some((bin, args)) = cmd.split_first() else {
        return;
    };
    let mut command = tokio::process::Command::new(*bin);
    command
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    command.kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return;
    };
    if tokio::time::timeout(timeout, child.wait()).await.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

async fn run_admin_timed_vec(argv: &[String], timeout: std::time::Duration) {
    let parts: Vec<&str> = argv.iter().map(String::as_str).collect();
    run_admin_timed(&parts, timeout).await;
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
            });
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(ContainerError::Canceled),
            _ = tokio::time::sleep(PROBE_INTERVAL) => {}
        }
    }
}

/// Blocking bounded subprocess for Drop paths.
fn run_sync_bounded(binary: &str, args: &[&str], timeout: std::time::Duration) {
    let mut command = std::process::Command::new(binary);
    command
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .env_clear();
    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", path);
    }
    let Ok(mut child) = command.spawn() else {
        return;
    };
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(_) => return,
        }
    }
}
