//! Managed-instance lifecycle shared by every caller: provision an
//! isolated instance, enter it for one-shot or long-lived workloads, and
//! always clean it up. Docker-compatible backends stay one-shot (`run`);
//! Incus-like backends follow init/configure/start/exec/
//! stop/delete with a guard that cleans up even when the awaiting future is
//! dropped by an outer timeout or abort.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::backend::Backend;
use super::error::ContainerError;
use super::plans::{self, Mount};

pub const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// How a live instance is addressed during cleanup.
#[derive(Debug, Clone)]
pub enum InstanceId {
    /// Docker-family tracking file holding the container id.
    CidFile(PathBuf),
    /// Managed instance name (Incus-like).
    Name(String),
}

/// A live container handle owned until [`teardown`] runs.
#[derive(Debug, Clone)]
pub struct Session {
    /// Deployment bundle captured before any daemon side effect.
    pub timeouts: policy::ContainerTimeouts,
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

/// Cumulative detached-cleanup failures observed since process start.
/// A failed backstop retry does not clear when a later attempt succeeds;
/// shutdown reports the count so unrecovered instances stay visible.
/// Awaited-path failures are recorded here too by the gateway, so the
/// shutdown report aggregates every cleanup failure, not just detached
/// ones.
static CLEANUP_FAILURES: AtomicUsize = AtomicUsize::new(0);

/// Outcome of awaiting detached cleanups and retained client reaps at
/// shutdown. `outstanding` means instances may still exist or local clients
/// remain unreaped; `failed` cleanups need operator attention.
/// Both are cumulative observations, never a proof of a clean stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupWaitOutcome {
    pub outstanding: usize,
    pub failed: usize,
}

/// Records one observed cleanup failure for shutdown aggregation.
/// Gateways call this when an awaited teardown fails (the guard backstop
/// stays armed for the retry); detached backstops record internally.
pub fn record_cleanup_failure() {
    CLEANUP_FAILURES.fetch_add(1, Ordering::AcqRel);
}

/// Waits until Drop-spawned cleanups and retained local client reaps finish,
/// up to `timeout`. Returns what is still outstanding plus the observed
/// failure count so shutdown reports unrecovered instances instead of claiming
/// a clean stop. Logs
/// the remainder instead of wedging shutdown forever.
pub async fn await_outstanding_cleanups(timeout: Duration) -> CleanupWaitOutcome {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = OUTSTANDING_CLEANUPS.load(Ordering::Acquire) + reap_pending_children();
        if remaining == 0 {
            return CleanupWaitOutcome {
                outstanding: 0,
                failed: CLEANUP_FAILURES.load(Ordering::Acquire),
            };
        }
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                remaining,
                "container cleanups still outstanding past shutdown deadline"
            );
            return CleanupWaitOutcome {
                outstanding: remaining,
                failed: CLEANUP_FAILURES.load(Ordering::Acquire),
            };
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Runs a blocking bounded teardown off the dropping thread and tracks it
/// for shutdown joining. Failures are logged with the instance identity;
/// an unspawnable thread falls back to inline teardown because blocking is
/// preferable to orphaning the instance.
fn spawn_detached_cleanup(session: Session, config_path: Option<PathBuf>) {
    spawn_cleanup_with(session, config_path, |cleanup| {
        std::thread::Builder::new()
            .name("container-teardown".into())
            .spawn(cleanup)
            .is_ok()
    });
}

fn spawn_cleanup_with(
    session: Session,
    config_path: Option<PathBuf>,
    spawn: impl FnOnce(Box<dyn FnOnce() + Send>) -> bool,
) {
    OUTSTANDING_CLEANUPS.fetch_add(1, Ordering::AcqRel);
    let thread_session = session.clone();
    let thread_config = config_path.clone();
    let spawned = spawn(Box::new(move || {
        if let Err(error) = teardown_sync(&thread_session) {
            record_cleanup_failure();
            tracing::warn!(%error, "detached container teardown failed");
        }
        if let Some(path) = &thread_config {
            let _ = std::fs::remove_file(path);
        }
        OUTSTANDING_CLEANUPS.fetch_sub(1, Ordering::AcqRel);
    }));
    if !spawned {
        #[cfg(test)]
        INLINE_CLEANUP_HOOK.with_borrow_mut(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
        if let Err(error) = teardown_sync(&session) {
            record_cleanup_failure();
            tracing::warn!(%error, "inline container teardown failed");
        }
        if let Some(path) = config_path {
            let _ = std::fs::remove_file(path);
        }
        OUTSTANDING_CLEANUPS.fetch_sub(1, Ordering::AcqRel);
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
    timeouts: policy::ContainerTimeouts,
    backend: Backend,
    name: String,
    config_path: Option<PathBuf>,
    committed: bool,
}

impl ProvisionGuard {
    fn new(
        backend: &Backend,
        name: String,
        config_path: Option<PathBuf>,
        timeouts: policy::ContainerTimeouts,
    ) -> Self {
        Self {
            backend: backend.clone(),
            timeouts,
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
            timeouts: self.timeouts,
            id: InstanceId::Name(self.name.clone()),
        };
        spawn_detached_cleanup(session, self.config_path.clone());
    }
}

pub struct Provision<'a> {
    /// Deployment bundle captured before any daemon side effect.
    pub timeouts: policy::ContainerTimeouts,
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
                timeouts: spec.timeouts,
                id: InstanceId::CidFile(cidfile),
            })
        }
        Backend::Incus { binary } => provision_incus(binary, spec).await,
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
        spec.timeouts,
    );
    let cleanup = || Session {
        timeouts: spec.timeouts,
        backend: Backend::Incus {
            binary: binary.to_string(),
        },
        id: InstanceId::Name(name.clone()),
    };
    let result: Result<(), ContainerError> = async {
        // Storage pool from the daemon's own default profile.
        let pool_output = run_admin(
            &plans::incus_pool_argv(binary),
            spec.timeouts.admin(),
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
            spec.timeouts.create(),
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
                    &format!("work{index}"),
                    mount.host,
                    mount.guest,
                    mount.readonly,
                ),
                spec.timeouts.admin(),
                spec.cancel,
                &name,
                "configure-mount",
            )
            .await?;
        }
        run_admin(
            &plans::incus_secure_argv(binary, &name),
            spec.timeouts.admin(),
            spec.cancel,
            &name,
            "configure-security",
        )
        .await?;
        run_admin(
            &plans::incus_start_argv(binary, &name),
            spec.timeouts.start(),
            spec.cancel,
            &name,
            "start",
        )
        .await?;
        wait_ready(
            &plans::incus_probe_argv(binary, &name),
            spec.timeouts.start(),
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
            // An already-existing name belongs to another owner: our
            // create was refused before it made anything, so tearing the
            // name down would stop or delete a foreign instance (D01).
            if matches!(error, ContainerError::InstanceExists { .. }) {
                guard.disarm();
                return Err(error);
            }
            // Awaited cleanup first for observability; the guard backstop
            // stays armed only if this fails, so a settled provision is
            // never torn down twice.
            if teardown(&cleanup()).await.is_err() {
                record_cleanup_failure();
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

/// Whether daemon stderr proves the instance itself already exists (D01).
/// Requires the daemon's existing-name report for OUR instance: a generic
/// "already exists" about a device, pool, or foreign name proves nothing.
fn is_instance_exists_report(stderr: &str, instance: &str) -> bool {
    let lower = stderr.to_lowercase();
    if !lower.contains(&instance.to_lowercase()) {
        return false;
    }
    ["already exists", "already in use", "exists already"]
        .iter()
        .any(|marker| lower.contains(marker))
}

/// Reads a Docker-family tracking file. `Ok(None)` covers the states that
/// prove no container identity was ever recorded (missing file, empty
/// content); an unreadable file (invalid UTF-8, permissions, I/O) is an
/// error so teardown never reports success while the identity is unknown
/// (D06).
fn read_cidfile(cidfile: &std::path::Path) -> Result<Option<String>, ContainerError> {
    match std::fs::read_to_string(cidfile) {
        Ok(contents) => {
            let id = contents.trim().to_string();
            Ok((!id.is_empty()).then_some(id))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ContainerError::Io(error)),
    }
}

/// Awaited teardown on every exit path. Returns infrastructure failures
/// (spawn failure, daemon timeout, unexpected non-zero exit) so callers log
/// them with the instance identity; the Drop guard re-attempts via
/// [`teardown_sync`]. Stop/kill failures never abort the subsequent
/// delete/remove: teardown is idempotent, so "already stopped" must not
/// strand the instance. A daemon-confirmed absent instance
/// ([`ContainerError::InstanceAbsent`]) is success; any other failure keeps
/// the tracking identity for retry.
pub async fn teardown(session: &Session) -> Result<(), ContainerError> {
    match session {
        Session {
            backend: Backend::Docker { binary, .. },
            id: InstanceId::CidFile(cidfile),
            ..
        } => {
            let Some(id) = read_cidfile(cidfile)? else {
                let _ = std::fs::remove_file(cidfile);
                return Ok(());
            };
            if let Err(error) = run_admin_timed(
                &[binary.as_str(), "kill", &id],
                session.timeouts.stop(),
                &id,
                "kill",
            )
            .await
            {
                tracing::warn!(%error, instance = id.as_str(), "container kill failed; continuing to rm");
            }
            match run_admin_timed(
                &[binary.as_str(), "rm", "-f", &id],
                session.timeouts.stop(),
                &id,
                "rm",
            )
            .await
            {
                Ok(()) | Err(ContainerError::InstanceAbsent { .. }) => {}
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
            ..
        } => {
            if let Err(error) = run_admin_timed_vec(
                &plans::incus_stop_argv(binary, name),
                session.timeouts.stop(),
                name,
                "stop",
            )
            .await
            {
                tracing::warn!(%error, instance = name.as_str(), "instance stop failed; continuing to delete");
            }
            match run_admin_timed_vec(
                &plans::incus_delete_argv(binary, name),
                session.timeouts.stop(),
                name,
                "delete",
            )
            .await
            {
                Ok(()) | Err(ContainerError::InstanceAbsent { .. }) => Ok(()),
                Err(error) => Err(error),
            }
        }
        _ => Ok(()),
    }
}

/// Whether daemon stderr proves the instance itself is absent (C05).
/// Only the daemon's absence report for OUR instance counts: the report
/// must name the instance and use absence language, and must not be a
/// client-side failure (missing executable, unreachable socket) which
/// merely proves the teardown never ran. Spawn failures, timeouts, and
/// connection errors never reach this function — they are distinct error
/// variants at the call site — so a match here is a proof of absence,
/// not a guess.
fn is_daemon_absent_report(stderr: &str, instance: &str) -> bool {
    let lower = stderr.to_lowercase();
    // Client-side failures prove nothing about the instance.
    if lower.contains("file or directory") || lower.contains("socket") {
        return false;
    }
    let instance = instance.to_lowercase();
    // Keep the recognized daemon forms deliberately narrow: an absence
    // phrase must identify this exact instance in the same line. In
    // particular, a target name elsewhere in a multi-line diagnostic and
    // an unrelated resource's "not found" are not proof of absence.
    const FORMS: &[(&str, &str)] = &[
        ("no such container", ""),
        ("no such instance", ""),
        ("instance", "not found"),
        ("the instance", "not found"),
        ("instance", "does not exist"),
        ("the instance", "does not exist"),
        ("could not find container", ""),
    ];
    lower.lines().any(|line| {
        FORMS
            .iter()
            .any(|(prefix, suffix)| daemon_absence_line(line, prefix, suffix, &instance))
    })
}

fn daemon_absence_line(line: &str, prefix: &str, suffix: &str, instance: &str) -> bool {
    for (start, _) in line.match_indices(prefix) {
        if start > 0
            && line[..start]
                .chars()
                .next_back()
                .is_some_and(is_instance_name_char)
        {
            continue;
        }
        let after_prefix = line[start + prefix.len()..].trim_start();
        let after_prefix = after_prefix
            .strip_prefix(':')
            .unwrap_or(after_prefix)
            .trim_start();
        let Some(after_instance) = exact_instance_suffix(after_prefix, instance) else {
            continue;
        };
        let rest = after_instance.trim_start();
        if suffix.is_empty() {
            if only_report_punctuation(rest) {
                return true;
            }
        } else if let Some(rest) = rest.strip_prefix(suffix)
            && rest
                .chars()
                .next()
                .is_none_or(|ch| !ch.is_ascii_alphanumeric())
            && only_report_punctuation(rest.trim_start())
        {
            return true;
        }
    }
    false
}

fn exact_instance_suffix<'a>(text: &'a str, instance: &str) -> Option<&'a str> {
    if let Some(quote @ ('"' | '\'')) = text.chars().next() {
        let quoted = &text[quote.len_utf8()..];
        let end = quoted.find(quote)?;
        if quoted[..end] == *instance {
            return Some(&quoted[end + quote.len_utf8()..]);
        }
        return None;
    }
    let suffix = text.strip_prefix(instance)?;
    if suffix.chars().next().is_some_and(is_instance_name_char) {
        return None;
    }
    Some(suffix)
}

fn is_instance_name_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')
}

fn only_report_punctuation(text: &str) -> bool {
    text.chars()
        .all(|ch| ch.is_ascii_whitespace() || matches!(ch, '.' | ':' | ';' | ',' | '!'))
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
            ..
        } => {
            let Some(id) = read_cidfile(cidfile)? else {
                let _ = std::fs::remove_file(cidfile);
                return Ok(());
            };
            for (args, stage) in [
                (vec!["kill", id.as_str()], "kill"),
                (vec!["rm", "-f", id.as_str()], "rm"),
            ] {
                if let Err(error) =
                    run_sync_bounded(binary, &args, session.timeouts.stop(), &id, stage)
                {
                    if stage == "rm" && matches!(error, ContainerError::InstanceAbsent { .. }) {
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
            ..
        } => {
            for (argv, stage) in [
                (plans::incus_stop_argv(binary, name), "stop"),
                (plans::incus_delete_argv(binary, name), "delete"),
            ] {
                let Some((bin, args)) = argv.split_first() else {
                    continue;
                };
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                if let Err(error) =
                    run_sync_bounded(bin, &args, session.timeouts.stop(), name, stage)
                {
                    if matches!(error, ContainerError::InstanceAbsent { .. }) {
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
        _ => Ok(()),
    }
}

#[derive(Debug)]
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
    use tokio::io::AsyncReadExt as _;

    reap_pending_children();
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
    let mut child = AsyncChild::new(command.spawn()?);
    let mut stdout = child.child.as_mut().unwrap().stdout.take().unwrap();
    let mut stderr = child.child.as_mut().unwrap().stderr.take().unwrap();
    // Keep ownership outside the cancelable completion future. Both pipes
    // must drain concurrently with wait, just as with wait_with_output.
    let completion = async {
        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let (status, _, _) = tokio::try_join!(
            child.wait(),
            stdout.read_to_end(&mut stdout_bytes),
            stderr.read_to_end(&mut stderr_bytes),
        )?;
        Ok::<_, std::io::Error>(std::process::Output {
            status,
            stdout: stdout_bytes,
            stderr: stderr_bytes,
        })
    };
    let output = tokio::select! {
        _ = cancel.cancelled() => {
            return Err(ContainerError::Canceled);
        }
        output = tokio::time::timeout(timeout, completion) => {
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
        // Creation stages only: an existing-name report means the name
        // belongs to another provisioning attempt and must never be
        // adopted for teardown (D01).
        if matches!(stage, "init" | "create") && is_instance_exists_report(&stderr, instance) {
            return Err(ContainerError::InstanceExists {
                stage,
                instance: instance.to_string(),
            });
        }
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
    reap_pending_children();
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
    if let Ok(home) = std::env::var("HOME") {
        command.env("HOME", home);
    }
    command.kill_on_drop(true);
    let child = command
        .spawn()
        .map_err(|error| ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: format!("teardown command failed to spawn: {error}"),
        })?;
    let mut child = AsyncChild::new(child);
    // Drain while waiting: even a healthy client can fill its stderr pipe.
    // EOF is part of completion, since descendants can inherit that pipe.
    let stderr = child.child.as_mut().unwrap().stderr.take();
    let completion = async { tokio::join!(child.wait(), collect_teardown_stderr(stderr)) };
    let (status, stderr) = match tokio::time::timeout(timeout, completion).await {
        Ok((Ok(status), stderr)) => (status, stderr),
        Ok((Err(error), _)) => {
            return Err(ContainerError::StageFailed {
                stage,
                instance: instance.to_string(),
                detail: format!("teardown wait failed: {error}"),
            });
        }
        Err(_) => {
            // The daemon deadline has expired; the collector (and its pipe)
            // is already dropped. Only local kill/reap work remains. Do not
            // leave reaping to a runtime that sync cleanup is about to destroy.
            if let Some(process) = child.child.as_mut() {
                if let Err(error) = process.start_kill() {
                    tracing::warn!(%error, instance, "timed-out teardown child kill failed");
                }
                let _ = tokio::time::timeout(POST_KILL_REAP_TIMEOUT, child.wait()).await;
            }
            // Any unreaped child is transferred by Drop, including if this
            // post-kill wait is itself canceled. A reaped child waiting only
            // for inherited stderr needs no further local cleanup.
            return Err(ContainerError::StageTimedOut {
                stage,
                instance: instance.to_string(),
                timeout_secs: timeout.as_secs(),
            });
        }
    };
    if !status.success() {
        if stderr.proves_absent(instance) {
            return Err(ContainerError::InstanceAbsent {
                stage,
                instance: instance.to_string(),
            });
        }
        return Err(ContainerError::StageFailed {
            stage,
            instance: instance.to_string(),
            detail: format!("exit {status}: {}", stderr.detail()),
        });
    }
    Ok(())
}

// Diagnostic storage, not an output limit: continue draining beyond this
// bound so a verbose daemon cannot deadlock on a full pipe. Truncated,
// missing, invalid UTF-8 or partially read output is never absence proof;
// in particular a discarded prefix might contain a client-side error.
const TEARDOWN_STDERR_CAPACITY: usize = 16 * 1024;

#[derive(Default)]
struct TeardownStderr {
    tail: Vec<u8>,
    complete: bool,
    truncated: bool,
}

impl TeardownStderr {
    fn push(&mut self, bytes: &[u8]) {
        let excess = (self.tail.len() + bytes.len()).saturating_sub(TEARDOWN_STDERR_CAPACITY);
        if excess > 0 {
            self.truncated = true;
            self.tail.drain(..excess.min(self.tail.len()));
        }
        self.tail
            .extend_from_slice(&bytes[bytes.len().saturating_sub(TEARDOWN_STDERR_CAPACITY)..]);
    }

    fn proves_absent(&self, instance: &str) -> bool {
        self.complete
            && !self.truncated
            && std::str::from_utf8(&self.tail)
                .is_ok_and(|text| is_daemon_absent_report(text, instance))
    }

    fn detail(&self) -> String {
        let note = if !self.complete {
            "[stderr missing or unreadable] "
        } else if self.truncated {
            "[stderr truncated] "
        } else {
            ""
        };
        format!("{note}{}", stderr_tail(&self.tail))
    }
}

async fn collect_teardown_stderr(
    pipe: Option<impl tokio::io::AsyncRead + Unpin>,
) -> TeardownStderr {
    use tokio::io::AsyncReadExt as _;
    let mut captured = TeardownStderr::default();
    let Some(mut pipe) = pipe else {
        return captured;
    };
    let mut buffer = [0; 4096];
    loop {
        match pipe.read(&mut buffer).await {
            Ok(0) => {
                captured.complete = true;
                return captured;
            }
            Ok(count) => captured.push(&buffer[..count]),
            Err(_) => return captured,
        }
    }
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
    reap_pending_children();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cancel.is_cancelled() {
            return Err(ContainerError::Canceled);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ContainerError::StageTimedOut {
                stage: "wait-ready",
                instance: instance.to_string(),
                timeout_secs: timeout.as_secs(),
            });
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
        if let Ok(child) = command.spawn() {
            let mut child = AsyncChild::new(child);
            let status = tokio::select! {
                _ = cancel.cancelled() => return Err(ContainerError::Canceled),
                status = tokio::time::timeout_at(deadline, child.wait()) => status,
            };
            if matches!(status, Ok(Ok(status)) if status.success()) {
                return Ok(());
            }
            // Drop kills and retains an unreaped probe without awaiting OS
            // cleanup past the global start deadline (or on wait failure).
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(ContainerError::Canceled),
            _ = tokio::time::sleep_until(
                (tokio::time::Instant::now() + PROBE_INTERVAL).min(deadline)
            ) => {}
        }
    }
}

// A killed client normally reaps immediately. Allow at most 100 ms of
// local post-kill work per command, separate from the daemon operation
// deadline. This never extends the configured shutdown wait budget. If the
// OS cannot reap yet (e.g. uninterruptible I/O), retain explicit ownership
// outside any temporary runtime; subsequent commands and the shutdown join
// poll these handles with try_wait. This covers direct CLI children used for
// provisioning, readiness and teardown, not their descendants. Cancellation
// hands off immediately rather than extending a daemon deadline with a wait.
// Pending reaps count as outstanding at shutdown, never as a clean stop. No
// extra thread is needed, even under thread exhaustion.
const POST_KILL_REAP_TIMEOUT: Duration = Duration::from_millis(100);
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(5);
static PENDING_REAPS: Mutex<Vec<ReapChild>> = Mutex::new(Vec::new());

/// Owns a direct CLI child across every await, including any bounded
/// post-kill wait. Tokio's kill-on-drop alone can leave reaping dependent on
/// a runtime that the caller is about to destroy. Transfer that ownership
/// explicitly on cancellation or wait failure; only a successful wait disarms.
struct AsyncChild {
    child: Option<tokio::process::Child>,
}

impl AsyncChild {
    fn new(child: tokio::process::Child) -> Self {
        Self { child: Some(child) }
    }

    async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let status = self.child.as_mut().unwrap().wait().await?;
        self.child = None;
        Ok(status)
    }
}

impl Drop for AsyncChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Err(error) = child.start_kill() {
                tracing::warn!(%error, "unreaped CLI child kill failed");
            }
            retain_for_reaping(ReapChild::Async(Box::new(child)));
        }
    }
}

enum ReapChild {
    Async(Box<tokio::process::Child>),
    Sync(std::process::Child),
}

impl ReapChild {
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match self {
            Self::Async(child) => child.try_wait(),
            Self::Sync(child) => child.try_wait(),
        }
    }
}

fn retain_for_reaping(child: ReapChild) {
    tracing::warn!("killed CLI child not yet reaped; retaining for shutdown join");
    PENDING_REAPS.lock().unwrap().push(child);
}

fn reap_pending_children() -> usize {
    let mut pending = PENDING_REAPS.lock().unwrap();
    pending.retain_mut(|child| !matches!(child.try_wait(), Ok(Some(_))));
    pending.len()
}

#[cfg(test)]
std::thread_local! {
    static FAIL_TEARDOWN_WORKER_SPAWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static INLINE_CLEANUP_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// Blocking bounded subprocess for Drop paths. A non-zero exit is a
/// failure with the exit code attached; callers map "already absent" to
/// success only when a complete stderr report proves it.
fn run_sync_bounded(
    binary: &str,
    args: &[&str],
    timeout: std::time::Duration,
    instance: &str,
    stage: &'static str,
) -> Result<(), ContainerError> {
    reap_pending_children();
    // A blocking pipe reader cannot be canceled portably when a descendant
    // inherits stderr. Reuse the async collector on a short-lived thread,
    // permitting sync cleanup from inside an existing Tokio runtime too.
    let failed = |detail: String| ContainerError::StageFailed {
        stage,
        instance: instance.to_string(),
        detail,
    };
    std::thread::scope(|scope| {
        let work = || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(%error, "teardown runtime unavailable; attempting command without stderr");
                    return run_sync_without_stderr(binary, args, timeout, instance, stage);
                }
            };
            let cmd: Vec<&str> = std::iter::once(binary)
                .chain(args.iter().copied())
                .collect();
            runtime.block_on(run_admin_timed(&cmd, timeout, instance, stage))
        };
        #[cfg(test)]
        let fail_spawn = FAIL_TEARDOWN_WORKER_SPAWN.get();
        #[cfg(not(test))]
        let fail_spawn = false;
        let worker = if fail_spawn {
            Err(std::io::Error::other(
                "injected teardown worker spawn failure",
            ))
        } else {
            std::thread::Builder::new()
                .name("container-teardown-command".into())
                .spawn_scoped(scope, work)
        };
        match worker {
            Ok(worker) => worker
                .join()
                .map_err(|_| failed("teardown worker panicked".into()))?,
            Err(error) => {
                tracing::warn!(%error, "teardown worker unavailable; attempting command without stderr");
                run_sync_without_stderr(binary, args, timeout, instance, stage)
            }
        }
    })
}

/// Last resort when even the command worker cannot spawn. Null stderr
/// cannot fill a pipe, needs no reader thread, and cannot prove absence.
/// Exit zero still proves command success; all nonzero exits fail closed.
fn run_sync_without_stderr(
    binary: &str,
    args: &[&str],
    timeout: Duration,
    instance: &str,
    stage: &'static str,
) -> Result<(), ContainerError> {
    let failed = |detail: String| ContainerError::StageFailed {
        stage,
        instance: instance.to_string(),
        detail,
    };
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
    if let Ok(home) = std::env::var("HOME") {
        command.env("HOME", home);
    }
    let mut child = command
        .spawn()
        .map_err(|error| failed(format!("teardown command failed to spawn: {error}")))?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            Ok(Some(status)) => {
                return Err(failed(format!("exit {status}: [stderr unavailable]")));
            }
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(
                    CHILD_POLL_INTERVAL
                        .min(deadline.saturating_duration_since(std::time::Instant::now())),
                );
            }
            outcome => {
                let result = match outcome {
                    Err(error) => failed(format!("teardown wait failed: {error}")),
                    _ => ContainerError::StageTimedOut {
                        stage,
                        instance: instance.to_string(),
                        timeout_secs: timeout.as_secs(),
                    },
                };
                if let Err(error) = child.kill() {
                    tracing::warn!(%error, instance, "teardown child kill failed");
                }
                let reap_deadline = std::time::Instant::now() + POST_KILL_REAP_TIMEOUT;
                loop {
                    if matches!(child.try_wait(), Ok(Some(_))) {
                        break;
                    }
                    if std::time::Instant::now() >= reap_deadline {
                        retain_for_reaping(ReapChild::Sync(child));
                        break;
                    }
                    std::thread::sleep(CHILD_POLL_INTERVAL);
                }
                return Err(result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stderr_proof_requires_complete_readable_untruncated_output() {
        use tokio::io::AsyncReadExt as _;

        struct Unreadable;
        impl tokio::io::AsyncRead for Unreadable {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::Error::other("unreadable stderr")))
            }
        }

        let report = b"no such instance owned-id";
        let complete = collect_teardown_stderr(Some(&report[..])).await;
        assert!(complete.proves_absent("owned-id"));
        let partial = collect_teardown_stderr(Some((&report[..]).chain(Unreadable))).await;
        assert!(!partial.complete);
        assert!(!partial.proves_absent("owned-id"));
        assert!(partial.detail().contains("unreadable"));
        let missing = collect_teardown_stderr(None::<&[u8]>).await;
        assert!(!missing.proves_absent("owned-id"));

        let mut large = TeardownStderr::default();
        large.push(&vec![b'x'; 2 * TEARDOWN_STDERR_CAPACITY]);
        large.push(report);
        large.complete = true;
        assert_eq!(large.tail.len(), TEARDOWN_STDERR_CAPACITY);
        assert!(large.truncated);
        assert!(large.tail.ends_with(report));
        assert!(!large.proves_absent("owned-id"));
        assert!(large.detail().len() < 1024);
    }

    fn bogus_docker_session(id_contents: &str) -> (tempfile_like::TempCid, Session) {
        tempfile_like::TempCid::create(id_contents)
    }

    fn bogus_docker_session_bytes(id_contents: &[u8]) -> (tempfile_like::TempCid, Session) {
        tempfile_like::TempCid::create_bytes(id_contents)
    }

    mod tempfile_like {
        use super::*;

        pub(crate) struct TempCid {
            pub(crate) path: PathBuf,
        }

        impl TempCid {
            pub(crate) fn create(contents: &str) -> (TempCid, Session) {
                Self::create_bytes(contents.as_bytes())
            }

            pub(crate) fn create_bytes(contents: &[u8]) -> (TempCid, Session) {
                let path = std::env::temp_dir()
                    .join(format!(".test-cid-{}", uuid::Uuid::now_v7().as_simple()));
                std::fs::write(&path, contents).expect("test cidfile should be written");
                let session = Session {
                    timeouts: policy::ContainerTimeouts::default(),
                    backend: Backend::Docker {
                        binary: "definitely-not-a-binary".into(),
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
        // instead of deleting the evidence: the error identifies the
        // instance and the tracking file survives on disk.
        let (cid, session) = bogus_docker_session("deadbeef");
        let error = teardown_sync(&session).expect_err("bogus binary must fail teardown");
        assert!(
            error.to_string().contains("deadbeef") || error.to_string().contains("failed to spawn"),
            "failure must identify the instance, got: {error}"
        );
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
    fn unreadable_tracking_file_teardown_fails_and_preserves_evidence() {
        // D06: invalid UTF-8 must not read as an empty id (success). The
        // identity is unknown, so teardown fails and keeps the file for
        // the operator instead of claiming the instance is gone.
        let (cid, session) = bogus_docker_session_bytes(&[0xff, 0xfe, 0x00]);
        let error = teardown_sync(&session).expect_err("unreadable id must not read as success");
        assert!(
            matches!(error, ContainerError::Io(_)),
            "unreadable tracking file must surface as an I/O failure, got: {error}"
        );
        assert!(
            cid.path.exists(),
            "unreadable tracking file must survive for operator recovery"
        );
    }

    #[test]
    fn instance_exists_reports_are_recognized() {
        // D01: only an existing-name report naming OUR instance classifies;
        // device, pool, or foreign-name chatter must not adopt another
        // owner's name for teardown.
        for report in [
            "Error: Failed instance creation: The instance \"1\" already exists",
            "error: instance ctr-1 already exists",
            "container ctr-1 exists already",
        ] {
            assert!(
                is_instance_exists_report(report, "1"),
                "{report} must read as our instance existing"
            );
        }
        for report in [
            "Error: Device \"work0\" already exists",
            "Error: The instance \"2\" already exists",
            "Error: no such instance ctr-1",
            "Error: pool default already exists",
        ] {
            assert!(
                !is_instance_exists_report(report, "1"),
                "{report} must not read as our instance existing"
            );
        }
    }

    async fn run_admin_stage(
        stage: &'static str,
        message: &str,
    ) -> Result<AdminOutput, ContainerError> {
        let cancel = CancellationToken::new();
        // Portable failing helper: stderr carries the report, exit is
        // non-zero. Messages avoid shell metacharacters on both sides.
        #[cfg(unix)]
        let argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            format!("echo '{message}' >&2; exit 1"),
        ];
        #[cfg(windows)]
        let argv = vec![
            "cmd".to_string(),
            "/C".to_string(),
            format!("echo {message} 1>&2 & exit 1"),
        ];
        run_admin(
            &argv,
            std::time::Duration::from_secs(10),
            &cancel,
            "1",
            stage,
        )
        .await
    }

    #[tokio::test]
    async fn create_stages_classify_existing_names_without_touching_other_stages() {
        // D01: only the create/init stages read an existing-name report as
        // ownership evidence; later stages keep the generic failure so a
        // device or config failure is never mistaken for another owner.
        for stage in ["init", "create"] {
            let error = run_admin_stage(stage, "Error: The instance ctr-1 already exists")
                .await
                .expect_err("existing name must fail");
            assert!(
                matches!(error, ContainerError::InstanceExists { stage: s, .. } if s == stage),
                "{stage} must classify as InstanceExists, got: {error}"
            );
        }
        let error = run_admin_stage("start", "Error: The instance ctr-1 already exists")
            .await
            .expect_err("start must still fail");
        assert!(
            matches!(error, ContainerError::StageFailed { stage: "start", .. }),
            "non-create stages must keep the generic failure, got: {error}"
        );
    }

    #[test]
    fn absent_instance_reports_are_recognized() {
        // C05: only the daemon naming OUR instance absent proves there is
        // nothing to tear down. Client-side failures (missing executable,
        // unreachable socket) and nameless reports stay failures so a
        // live instance is never declared gone.
        for (report, instance) in [
            ("Error: No such container: ctr-1", "ctr-1"),
            (
                "Error response from daemon: No such container: ctr-1",
                "ctr-1",
            ),
            ("Error: Instance \"1\" not found", "1"),
            ("error: The instance ctr-1 does not exist", "ctr-1"),
            ("could not find container ctr-1", "ctr-1"),
        ] {
            assert!(
                is_daemon_absent_report(report, instance),
                "{report} must read as absent for {instance}"
            );
        }
        for report in [
            // Audit counterexamples: these prove the teardown never ran.
            "No such file or directory (os error 2)",
            "cannot connect to daemon socket: no such file or directory",
            "teardown command failed to spawn",
            "permission denied",
            "exit status: 1: ",
            "daemon timed out",
            // Nameless or foreign-instance reports prove nothing about ours.
            "Error response from daemon: No such object",
            "Instance not found",
            "Error: No such container: ctr-2",
            "No such container: ctr-10",
            "No such instance: owned-identifier",
            "Failed deleting instance owned-id: storage pool default not found",
            "Failed deleting owned-id\nError: project default does not exist",
        ] {
            assert!(
                !is_daemon_absent_report(report, "1"),
                "{report} must stay a failure"
            );
        }
    }

    #[tokio::test]
    async fn dropped_provision_guard_cleanup_is_joinable() {
        // B06: ownership held across a dropped provision still cleans up,
        // and shutdown can await the detached work.
        let guard = ProvisionGuard::new(
            &Backend::Incus {
                binary: "definitely-not-a-binary".into(),
            },
            format!("test{}", uuid::Uuid::now_v7().as_simple()),
            None,
            policy::ContainerTimeouts::default(),
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

    #[tokio::test]
    async fn failed_detached_cleanup_is_counted_for_shutdown() {
        // C05: a finished cleanup thread is not proof the instance is
        // gone. Failures aggregate separately so shutdown reports them.
        let before = await_outstanding_cleanups(std::time::Duration::from_secs(5)).await;
        // Keep the tracking file alive until the detached backstop has
        // finished: dropping TempCid first would let the cleanup observe a
        // missing file as success and record nothing.
        let (_cid, session) = bogus_docker_session("deadbeef");
        {
            // Drop the guard: the detached backstop fails on the bogus
            // binary and must record the failure.
            let _guard = SessionGuard::new(session);
        }
        let after = await_outstanding_cleanups(std::time::Duration::from_secs(30)).await;
        assert_eq!(
            after.outstanding, 0,
            "detached cleanup must complete before shutdown proceeds"
        );
        assert!(
            after.failed > before.failed,
            "failed backstop cleanup must aggregate for shutdown reporting"
        );
    }
}

#[cfg(all(test, unix))]
#[path = "../test_support/mod.rs"]
mod daemon_policy_test_support;

#[cfg(all(test, unix))]
mod daemon_policy_tests {
    use super::daemon_policy_test_support::Daemon;
    use super::*;

    #[tokio::test]
    async fn captured_policy_reaches_every_cleanup_path() {
        let Some(daemon) = Daemon::isolated(
            "session::daemon_policy_tests::captured_policy_reaches_every_cleanup_path",
        ) else {
            return;
        };
        let timeouts = policy::ContainerTimeouts::from_seconds([1; 4]).unwrap();
        for docker in [false, true] {
            for path in ["async", "sync", "drop", "spawn-failure"] {
                daemon.mode("hang-stop");
                let id = if docker {
                    let file = daemon.root.join("cid");
                    std::fs::write(&file, "owned-id").unwrap();
                    InstanceId::CidFile(file)
                } else {
                    InstanceId::Name("owned-name".into())
                };
                let session = Session {
                    timeouts,
                    backend: if docker {
                        Backend::Docker {
                            binary: "docker".into(),
                            runtime_flag: None,
                        }
                    } else {
                        Backend::Incus {
                            binary: "incus".into(),
                        }
                    },
                    id,
                };
                let started = std::time::Instant::now();
                match path {
                    "async" => teardown(&session).await.unwrap(),
                    "sync" => teardown_sync(&session).unwrap(),
                    "drop" => {
                        drop(SessionGuard::new(session));
                        assert_eq!(
                            await_outstanding_cleanups(
                                daemon_policy_test_support::CLEANUP_JOIN_BOUND,
                            )
                            .await
                            .outstanding,
                            0
                        );
                    }
                    "spawn-failure" => spawn_cleanup_with(session, None, |_| false),
                    _ => unreachable!(),
                }
                assert!(
                    started.elapsed() < Duration::from_secs(4),
                    "{path} ignored frozen stop timeout"
                );
                assert!(daemon.log().contains(if docker {
                    "rm -f owned-id"
                } else {
                    "delete -f owned-name"
                }));
            }
            daemon.mode("hang-delete");
            let session = if docker {
                let file = daemon.root.join("cid-delete");
                std::fs::write(&file, "owned-id").unwrap();
                Session {
                    timeouts,
                    backend: Backend::Docker {
                        binary: "docker".into(),
                        runtime_flag: None,
                    },
                    id: InstanceId::CidFile(file),
                }
            } else {
                Session {
                    timeouts,
                    backend: Backend::Incus {
                        binary: "incus".into(),
                    },
                    id: InstanceId::Name("owned-name".into()),
                }
            };
            for sync in [false, true] {
                let start = std::time::Instant::now();
                let error = if sync {
                    teardown_sync(&session)
                } else {
                    teardown(&session).await
                }
                .unwrap_err();
                assert!(matches!(
                    error,
                    ContainerError::StageTimedOut {
                        timeout_secs: 1,
                        ..
                    }
                ));
                assert!(start.elapsed() < Duration::from_secs(4));
            }
        }

        // Provision keeps the complete bundle on successful transfer too.
        daemon.mode("ok");
        let cancel = CancellationToken::new();
        let backend = Backend::Incus {
            binary: "incus".into(),
        };
        let image = format!("images:alpine@sha256:{}", "a".repeat(64));
        let spec = Provision {
            timeouts,
            image: &image,
            mounts: &[],
            id_prefix: "test",
            cancel: &cancel,
        };
        let session = provision(&backend, &spec).await.unwrap();
        assert_eq!(session.timeouts, timeouts);
        teardown(&session).await.unwrap();

        // Abort during init: ProvisionGuard must already own the stop policy.
        daemon.mode("hang-init");
        let mut pending = Box::pin(provision(&backend, &spec));
        tokio::select! {
            _ = &mut pending => panic!("init should still be running"),
            _ = daemon.wait_for("init ") => {}
        }
        daemon.mode("hang-stop");
        let start = std::time::Instant::now();
        drop(pending);
        assert_eq!(
            await_outstanding_cleanups(daemon_policy_test_support::CLEANUP_JOIN_BOUND)
                .await
                .outstanding,
            0
        );
        assert!(start.elapsed() < Duration::from_secs(4));
        assert!(daemon.log().contains("delete -f"));
    }
}

#[cfg(all(test, unix))]
#[path = "../test_support/cleanup_stderr.rs"]
mod cleanup_stderr_tests;
