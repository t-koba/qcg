use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::CommandPermission;
use qcg_policy::is_safe_relative_path;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use super::error::GatewayError;
use super::fs::CommandPermissionSummary;

pub(crate) enum StreamReadError {
    LimitExceeded,
    Io(std::io::Error),
}

pub(crate) async fn read_stream_bounded<R>(
    mut stream: R,
    used: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    limit: Option<usize>,
) -> Result<Vec<u8>, StreamReadError>
where
    R: AsyncRead + Unpin,
{
    use std::sync::atomic::Ordering;

    const CHUNK_SIZE: usize = 16 * 1024;
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; CHUNK_SIZE];
    loop {
        let read = stream.read(&mut chunk).await.map_err(StreamReadError::Io)?;
        if read == 0 {
            return Ok(bytes);
        }
        if let Some(limit) = limit {
            let reserved = used.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(read).filter(|next| *next <= limit)
            });
            if reserved.is_err() {
                return Err(StreamReadError::LimitExceeded);
            }
        } else {
            used.fetch_add(read, Ordering::AcqRel);
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
}

pub(crate) fn resolve_command_program(
    workspace: &Utf8Path,
    bin: &str,
) -> Result<std::path::PathBuf, GatewayError> {
    let path = Utf8Path::new(bin);
    if path.is_absolute() || !bin.contains('/') {
        return Ok(bin.into());
    }
    let relative = bin.strip_prefix("./").unwrap_or(bin);
    if !is_safe_relative_path(relative) {
        return Err(GatewayError::CommandPathDenied { bin: bin.into() });
    }
    let workspace = dunce::canonicalize(workspace)?;
    let program = dunce::canonicalize(workspace.join(relative))?;
    if !program.starts_with(&workspace) {
        return Err(GatewayError::CommandPathDenied { bin: bin.into() });
    }
    Ok(program)
}

pub(crate) fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command
            .as_std_mut()
            .creation_flags(CREATE_NEW_PROCESS_GROUP);
    }
}

pub(crate) struct ProcessTreeGuard {
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
    /// Process group id (`child.pid` via `process_group(0)`) killed on drop
    /// unless disarmed after a clean wait. Covers outer-timeout future drops
    /// that bypass the explicit cancel/timeout paths.
    #[cfg(unix)]
    pgid: Option<i32>,
    #[cfg(unix)]
    disarmed: bool,
}

impl ProcessTreeGuard {
    pub(crate) fn attach(child: &tokio::process::Child) -> Result<Self, std::io::Error> {
        #[cfg(windows)]
        {
            use std::ffi::c_void;
            use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
            use windows_sys::Win32::System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            };

            // SAFETY: all handles and pointers are valid for the duration of each Win32 call.
            unsafe {
                let raw_job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if raw_job.is_null() {
                    return Err(std::io::Error::last_os_error());
                }
                let job = OwnedHandle::from_raw_handle(raw_job);
                let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
                information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                let configured = SetInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectExtendedLimitInformation,
                    (&raw const information).cast::<c_void>(),
                    u32::try_from(std::mem::size_of_val(&information)).map_err(|_| {
                        std::io::Error::other("Windows Job Object information is too large")
                    })?,
                );
                if configured == 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let Some(process) = child.raw_handle() else {
                    return Err(std::io::Error::other("child process handle is unavailable"));
                };
                if AssignProcessToJobObject(job.as_raw_handle(), process) == 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(Self { job })
            }
        }
        #[cfg(not(windows))]
        {
            // OS pids always fit i32 (pid_max is far below 2^31); a
            // conversion failure only skips the group kill while the owned
            // child handle still terminates the child itself.
            let pgid = child.id().and_then(|pid| i32::try_from(pid).ok());
            Ok(Self {
                pgid,
                disarmed: false,
            })
        }
    }

    /// Marks the tree as cleanly reaped so Drop stays silent. Call after a
    /// successful wait; without it Drop would signal a possibly recycled
    /// process group id.
    pub(crate) fn disarm(&mut self) {
        #[cfg(unix)]
        {
            self.disarmed = true;
        }
    }

    pub(crate) async fn terminate(&mut self, child: &mut tokio::process::Child, pid: Option<u32>) {
        #[cfg(unix)]
        if let Some(pid) = pid
            && let Ok(pid) = i32::try_from(pid)
        {
            // SAFETY: the child was spawned into a new process group whose id is its pid.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::System::JobObjects::TerminateJobObject;
            // SAFETY: the Job Object handle remains owned by this guard.
            unsafe {
                TerminateJobObject(self.job.as_raw_handle(), 1);
            }
            let _ = pid;
        }
        let _ = child.kill().await;
        let _ = child.wait().await;
        self.disarm();
    }
}

#[cfg(unix)]
impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        if let Some(pgid) = self.pgid {
            // SAFETY: best-effort group kill for paths that never reached an
            // explicit wait (outer timeout dropping the future). The child
            // always starts in its own group (`process_group(0)` fails the
            // spawn otherwise), so our own group can never be the target.
            // Signal only while the group still contains our child
            // (`getpgid(child) == pgid` with pgid == child pid): a reaped
            // child frees the number, and a recycled pid in another group
            // (or no such pid, ESRCH) skips the kill instead of signaling
            // strangers. Residual risk is a recycled pid leading a new
            // group under the same number, which no portable POSIX check
            // can distinguish (Linux-only pidfd is unavailable here).
            unsafe {
                if libc::getpgid(pgid) == pgid {
                    libc::killpg(pgid, libc::SIGKILL);
                }
            }
        }
    }
}

pub(crate) fn command_permission_summaries(
    permissions: &[CommandPermission],
) -> Vec<CommandPermissionSummary> {
    permissions.iter().map(command_permission_summary).collect()
}

pub(crate) fn command_permission_summary(
    permission: &CommandPermission,
) -> CommandPermissionSummary {
    CommandPermissionSummary {
        bin: permission.bin.clone(),
        args: permission.args.clone(),
        purpose: permission.purpose.clone(),
        isolation: permission.isolation.clone(),
        image: permission.image.clone(),
    }
}

/// Static wildcard check without run roots (fail-closed for absolutes).
/// Test-only helper preserving the original contract: bare absolute paths
/// never match `*`. Production gateway code uses [`args_allowed_in`] with
/// the run workspace/snapshot roots.
#[cfg(test)]
pub(crate) fn args_allowed(permission: &CommandPermission, args: &[String]) -> bool {
    args_allowed_in(permission, args, &[])
}

/// Wildcard check with run-private absolute roots (E13). A static `*`
/// declaration keeps rejecting bare absolutes (fail-closed); when the
/// caller supplies the run workspace/snapshot roots, an absolute path
/// rooted in those directories is accepted like its workspace-relative
/// form. Anything outside the roots, or with `..`, NUL, or backslashes,
/// is still rejected, so `/etc/passwd` can never match `*`.
pub(crate) fn args_allowed_in(
    permission: &CommandPermission,
    args: &[String],
    allowed_absolute_roots: &[Utf8PathBuf],
) -> bool {
    if permission.args.is_empty() {
        return args.is_empty();
    }
    if permission.args.len() != args.len() {
        return false;
    }
    permission.args.iter().zip(args).all(|(pattern, actual)| {
        if pattern == actual {
            return true;
        }
        if pattern.contains('*') && !is_safe_wildcard_command_arg_in(actual, allowed_absolute_roots)
        {
            return false;
        }
        pattern == "*" || globish(pattern, actual)
    })
}

fn globish(pattern: &str, actual: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    match (pattern.strip_prefix('*'), pattern.strip_suffix('*')) {
        (Some(suffix), _) => actual.ends_with(suffix),
        (_, Some(prefix)) => actual.starts_with(prefix),
        _ => pattern == actual,
    }
}

/// Rooted safety check shared by the static validator and the run-private
/// gateway check. Relative paths follow the existing rules; absolute paths
/// are accepted only when lexically rooted in one of the supplied
/// run-private directories (workspace or snapshot/metadata) and otherwise
/// safe. Lexical prefixing is sufficient here: `..` components are rejected
/// outright, so `/root/../escape` can never pass, and the roots themselves
/// are per-run directories the validator already trusts for relative `*`.
fn is_safe_wildcard_command_arg_in(actual: &str, allowed_absolute_roots: &[Utf8PathBuf]) -> bool {
    if actual.is_empty() || actual.contains('\0') || actual.contains('\\') {
        return false;
    }
    if let Some((_, value)) = actual.split_once('=') {
        if value.is_empty() {
            return false;
        }
        // An `=` value may itself be an absolute run-private path (for
        // example `--input=/run/meta/snapshot/file`); otherwise it follows
        // the same relative rules as the whole arg.
        if value.starts_with('/') {
            return is_absolute_under_roots(value, allowed_absolute_roots)
                && !value.split('/').any(|part| part == "..");
        }
        if value.split('/').any(|part| part == "..") {
            return false;
        }
    }
    if actual.starts_with('/') {
        return is_absolute_under_roots(actual, allowed_absolute_roots)
            && !actual.split('/').any(|part| part == "..");
    }
    if actual.split('/').any(|part| part == "..") {
        return false;
    }
    true
}

/// Lexical root check: the absolute path must equal a root or start with
/// `root/`. No I/O is performed (the validator is pure); `..` is rejected
/// by the caller, so prefix matching cannot be escaped lexically.
fn is_absolute_under_roots(actual: &str, allowed_absolute_roots: &[Utf8PathBuf]) -> bool {
    allowed_absolute_roots.iter().any(|root| {
        let root = root.as_str().trim_end_matches('/');
        if root.is_empty() {
            return false;
        }
        actual == root || actual.starts_with(&format!("{root}/"))
    })
}
