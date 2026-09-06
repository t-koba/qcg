use camino::Utf8Path;
use qcg_contract::{CommandPermission, ContainerRuntime};
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

pub(crate) fn container_runtime_argv(runtime: &ContainerRuntime) -> Option<Vec<String>> {
    let path = std::env::var_os("PATH")?;
    let (binary, runtime_arg) = match runtime {
        ContainerRuntime::Docker => ("docker", None),
        ContainerRuntime::Podman => ("podman", None),
        ContainerRuntime::DockerRunsc => ("docker", Some("runsc")),
    };
    std::env::split_paths(&path)
        .any(|dir| dir.join(binary).is_file())
        .then(|| {
            let mut argv = vec![binary.to_string(), "run".into()];
            if let Some(runtime) = runtime_arg {
                argv.extend(["--runtime".into(), runtime.into()]);
            }
            argv
        })
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
            let _ = child;
            Ok(Self {})
        }
    }

    pub(crate) async fn terminate(&self, child: &mut tokio::process::Child, pid: Option<u32>) {
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

pub(crate) fn args_allowed(permission: &CommandPermission, args: &[String]) -> bool {
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
        if pattern.contains('*') && !is_safe_wildcard_command_arg(actual) {
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

fn is_safe_wildcard_command_arg(actual: &str) -> bool {
    if actual.is_empty()
        || actual.contains('\0')
        || actual.contains('\\')
        || actual.starts_with('/')
    {
        return false;
    }
    if actual.split('/').any(|part| part == "..") {
        return false;
    }
    if let Some((_, value)) = actual.split_once('=')
        && (value.is_empty() || value.starts_with('/') || value.split('/').any(|part| part == ".."))
    {
        return false;
    }
    true
}
