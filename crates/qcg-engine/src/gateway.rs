use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{
    CommandIsolation, CommandPermission, ContainerRuntime, Permissions, RuntimeLimits,
};
use qcg_types::{credential_like_name, is_safe_relative_path};
use reqwest::{Client, Method, redirect::Policy};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::process::Stdio;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("filesystem read from workspace is not allowed by permissions.fs_read")]
    FsReadDenied,
    #[error("filesystem write to workspace is not allowed by permissions.fs_write")]
    FsWriteDenied,
    #[error("path `{path}` is outside allowed workspace `{workspace}`")]
    PathDenied {
        path: Utf8PathBuf,
        workspace: Utf8PathBuf,
    },
    #[error("command is empty")]
    EmptyCommand,
    #[error(
        "command `{bin}` is not allowed by permissions.commands; allowed declarations: {allowed:?}"
    )]
    CommandDenied {
        bin: String,
        allowed: Vec<CommandPermissionSummary>,
    },
    #[error(
        "command `{bin}` arguments {actual:?} are not allowed by permissions.commands; allowed declarations: {allowed:?}"
    )]
    CommandArgsDenied {
        bin: String,
        actual: Vec<String>,
        allowed: Vec<CommandPermissionSummary>,
    },
    #[error("command path `{bin}` is not a safe executable inside the workspace")]
    CommandPathDenied { bin: String },
    #[error("command `{bin}` has no declared execution isolation")]
    CommandIsolationMissing { bin: String },
    #[error("container runtime was not found for command `{bin}`")]
    ContainerRuntimeMissing { bin: String },
    #[error("container-isolated command `{bin}` has no image")]
    ContainerImageMissing { bin: String },
    #[error("command `{bin}` timed out")]
    CommandTimedOut { bin: String },
    #[error("command `{bin}` output exceeded limit")]
    CommandOutputTooLarge { bin: String },
    #[error("command `{bin}` input exceeded limit")]
    CommandInputTooLarge { bin: String },
    #[error("network access to host `{host}` is not allowed by permissions.network")]
    NetworkDenied { host: String },
    #[error("unsupported URL `{url}`")]
    UnsupportedUrl { url: String },
    #[error("HTTP response body exceeded limit for `{url}`")]
    HttpBodyTooLarge { url: String },
    #[error("HTTP request body exceeded limit for `{url}`")]
    HttpRequestBodyTooLarge { url: String },
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("execution was canceled")]
    Canceled,
}

#[derive(Debug, Clone)]
pub struct CommandPermissionSummary {
    pub bin: String,
    pub args: Vec<String>,
    pub purpose: String,
    pub isolation: Option<CommandIsolation>,
    pub image: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FsGateway {
    workspace: Utf8PathBuf,
    can_read_workspace: bool,
    can_write_workspace: bool,
}

impl FsGateway {
    pub fn new(workspace: Utf8PathBuf, permissions: &Permissions) -> Self {
        Self {
            workspace,
            can_read_workspace: permissions.fs_read.iter().any(|scope| scope == "workspace"),
            can_write_workspace: permissions
                .fs_write
                .iter()
                .any(|scope| scope == "workspace"),
        }
    }

    pub fn resolve_read(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        if !self.can_read_workspace {
            return Err(GatewayError::FsReadDenied);
        }
        self.resolve_workspace_path(path, false)
    }

    pub fn resolve_write(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        if !self.can_write_workspace {
            return Err(GatewayError::FsWriteDenied);
        }
        let joined = self.resolve_workspace_path(path, true)?;
        ensure_parent(&joined)?;
        Ok(joined)
    }

    fn resolve_workspace_path(
        &self,
        path: &str,
        allow_missing_leaf: bool,
    ) -> Result<Utf8PathBuf, GatewayError> {
        if path == "." && !allow_missing_leaf {
            let workspace = dunce::canonicalize(&self.workspace)?;
            return Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        let relative = Utf8Path::new(path);
        if path.contains('\0')
            || path.contains('\\')
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, camino::Utf8Component::Normal(_)))
        {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        let joined = self.workspace.join(path);
        let workspace = dunce::canonicalize(&self.workspace)?;
        let candidate = if allow_missing_leaf {
            let parent = joined.parent().unwrap_or(&self.workspace);
            std::fs::create_dir_all(parent)?;
            let parent = dunce::canonicalize(parent)?;
            parent.join(joined.file_name().unwrap_or_default())
        } else {
            dunce::canonicalize(&joined)?
        };
        if !candidate.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        Utf8PathBuf::from_path_buf(candidate).map_err(|_| GatewayError::PathDenied {
            path: Utf8PathBuf::from(path),
            workspace: self.workspace.clone(),
        })
    }

    pub fn workspace(&self) -> &Utf8Path {
        &self.workspace
    }
}

fn ensure_parent(path: &Utf8Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CmdGateway {
    permissions: Permissions,
    limits: RuntimeLimits,
    workspace: Utf8PathBuf,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
    /// Raw stdout bytes retained for strict machine-readable command modes.
    pub stdout_bytes: Vec<u8>,
    /// Raw stderr bytes retained for diagnostics without lossy decoding.
    pub stderr_bytes: Vec<u8>,
}

impl CmdGateway {
    pub fn new(permissions: Permissions, workspace: Utf8PathBuf) -> Self {
        Self {
            permissions,
            limits: RuntimeLimits::default(),
            workspace,
            cancellation: CancellationToken::new(),
        }
    }

    pub fn with_limits(mut self, limits: RuntimeLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub async fn run(&self, argv: &[String]) -> Result<CommandOutput, GatewayError> {
        self.run_with_limits(
            argv,
            self.limits.command_timeout_seconds,
            self.limits.command_output_limit_bytes,
        )
        .await
    }

    pub async fn run_with_limits(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: usize,
    ) -> Result<CommandOutput, GatewayError> {
        self.run_with_limits_and_stdin(argv, timeout_seconds, output_limit_bytes, None)
            .await
    }

    pub async fn run_with_stdin(
        &self,
        argv: &[String],
        stdin: &[u8],
    ) -> Result<CommandOutput, GatewayError> {
        self.run_with_limits_and_stdin(
            argv,
            self.limits.command_timeout_seconds,
            self.limits.command_output_limit_bytes,
            Some(stdin),
        )
        .await
    }

    pub async fn run_with_limits_and_stdin(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: usize,
        stdin: Option<&[u8]>,
    ) -> Result<CommandOutput, GatewayError> {
        let permission = self.command_permission(argv)?;
        match permission.isolation.as_ref().ok_or_else(|| {
            GatewayError::CommandIsolationMissing {
                bin: permission.bin.clone(),
            }
        })? {
            CommandIsolation::TrustedHost => {
                self.run_trusted_process_with_stdin(
                    argv,
                    timeout_seconds,
                    output_limit_bytes,
                    stdin,
                )
                .await
            }
            CommandIsolation::Container => {
                self.run_container_process(
                    argv,
                    permission.image.as_deref().ok_or_else(|| {
                        GatewayError::ContainerImageMissing {
                            bin: permission.bin.clone(),
                        }
                    })?,
                    timeout_seconds,
                    output_limit_bytes,
                    stdin,
                )
                .await
            }
        }
    }

    async fn run_container_process(
        &self,
        argv: &[String],
        image: &str,
        timeout_seconds: u64,
        output_limit_bytes: usize,
        stdin: Option<&[u8]>,
    ) -> Result<CommandOutput, GatewayError> {
        let bin = argv.first().cloned().ok_or(GatewayError::EmptyCommand)?;
        let mut container_argv = container_runtime_argv(
            self.permissions
                .containers
                .runtime
                .as_ref()
                .ok_or_else(|| GatewayError::ContainerRuntimeMissing { bin: bin.clone() })?,
        )
        .ok_or_else(|| GatewayError::ContainerRuntimeMissing { bin: bin.clone() })?;
        let mount = format!("type=bind,src={},dst=/work", self.workspace);
        container_argv.extend([
            "--rm".into(),
            "--network".into(),
            "none".into(),
            "--read-only".into(),
            "--cap-drop".into(),
            "ALL".into(),
            "--security-opt".into(),
            "no-new-privileges".into(),
            "--pids-limit".into(),
            "256".into(),
            "--tmpfs".into(),
            "/tmp:rw,noexec,nosuid,size=64m".into(),
            "--mount".into(),
            mount,
            "--workdir".into(),
            "/work".into(),
            image.into(),
        ]);
        container_argv.extend_from_slice(argv);
        self.run_trusted_process_with_stdin(
            &container_argv,
            timeout_seconds,
            output_limit_bytes,
            stdin,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn run_trusted_process(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: usize,
    ) -> Result<CommandOutput, GatewayError> {
        self.run_trusted_process_with_stdin(argv, timeout_seconds, output_limit_bytes, None)
            .await
    }

    #[doc(hidden)]
    pub async fn run_trusted_process_with_stdin(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: usize,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<CommandOutput, GatewayError> {
        let (bin, args) = argv.split_first().ok_or(GatewayError::EmptyCommand)?;
        if stdin_bytes.is_some_and(|bytes| bytes.len() > self.limits.command_input_limit_bytes) {
            return Err(GatewayError::CommandInputTooLarge { bin: bin.clone() });
        }
        let program = resolve_command_program(&self.workspace, bin)?;
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(&self.workspace)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("TMPDIR", self.workspace.as_str())
            .stdin(if stdin_bytes.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        configure_process_group(&mut command);
        let mut child = command.spawn()?;
        let process_tree = ProcessTreeGuard::attach(&child)?;
        let pid = child.id();
        let stdin_task = if let Some(stdin_bytes) = stdin_bytes {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| std::io::Error::other("command stdin pipe was not available"))?;
            let stdin_bytes = stdin_bytes.to_vec();
            Some(tokio::spawn(async move {
                let result = stdin.write_all(&stdin_bytes).await;
                drop(stdin);
                result
            }))
        } else {
            None
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("command stdout pipe was not available"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| std::io::Error::other("command stderr pipe was not available"))?;
        let output_bytes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stdout_task = tokio::spawn(read_stream_bounded(
            stdout,
            output_bytes.clone(),
            output_limit_bytes,
        ));
        let stderr_task = tokio::spawn(read_stream_bounded(
            stderr,
            output_bytes,
            output_limit_bytes,
        ));
        let mut stdout_task = stdout_task;
        let mut stderr_task = stderr_task;
        let mut stdout_result: Option<Result<Vec<u8>, StreamReadError>> = None;
        let mut stderr_result: Option<Result<Vec<u8>, StreamReadError>> = None;
        let mut child_status: Option<std::process::ExitStatus> = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);

        enum CommandEvent {
            Canceled,
            TimedOut,
            Child(Result<std::process::ExitStatus, std::io::Error>),
            Stdout(Result<Result<Vec<u8>, StreamReadError>, tokio::task::JoinError>),
            Stderr(Result<Result<Vec<u8>, StreamReadError>, tokio::task::JoinError>),
        }

        macro_rules! stop_tasks {
            () => {{
                stdout_task.abort();
                stderr_task.abort();
                if let Some(stdin_task) = stdin_task.as_ref() {
                    stdin_task.abort();
                }
            }};
        }

        loop {
            if child_status.is_some() && stdout_result.is_some() && stderr_result.is_some() {
                break;
            }
            let event = if child_status.is_none() {
                tokio::select! {
                    _ = self.cancellation.cancelled() => CommandEvent::Canceled,
                    _ = tokio::time::sleep_until(deadline) => CommandEvent::TimedOut,
                    result = child.wait() => CommandEvent::Child(result),
                    result = &mut stdout_task, if stdout_result.is_none() => CommandEvent::Stdout(result),
                    result = &mut stderr_task, if stderr_result.is_none() => CommandEvent::Stderr(result),
                }
            } else {
                tokio::select! {
                    _ = self.cancellation.cancelled() => CommandEvent::Canceled,
                    _ = tokio::time::sleep_until(deadline) => CommandEvent::TimedOut,
                    result = &mut stdout_task, if stdout_result.is_none() => CommandEvent::Stdout(result),
                    result = &mut stderr_task, if stderr_result.is_none() => CommandEvent::Stderr(result),
                }
            };

            match event {
                CommandEvent::Canceled => {
                    process_tree.terminate(&mut child, pid).await;
                    stop_tasks!();
                    return Err(GatewayError::Canceled);
                }
                CommandEvent::TimedOut => {
                    process_tree.terminate(&mut child, pid).await;
                    stop_tasks!();
                    return Err(GatewayError::CommandTimedOut { bin: bin.clone() });
                }
                CommandEvent::Child(result) => match result {
                    Ok(status) => child_status = Some(status),
                    Err(error) => {
                        process_tree.terminate(&mut child, pid).await;
                        stop_tasks!();
                        return Err(GatewayError::Io(error));
                    }
                },
                CommandEvent::Stdout(result) => {
                    let result = result.map_err(|error| {
                        GatewayError::Io(std::io::Error::other(error.to_string()))
                    });
                    match result {
                        Ok(Ok(bytes)) => stdout_result = Some(Ok(bytes)),
                        Ok(Err(StreamReadError::LimitExceeded)) => {
                            process_tree.terminate(&mut child, pid).await;
                            stop_tasks!();
                            return Err(GatewayError::CommandOutputTooLarge { bin: bin.clone() });
                        }
                        Ok(Err(StreamReadError::Io(error))) => {
                            process_tree.terminate(&mut child, pid).await;
                            stop_tasks!();
                            return Err(GatewayError::Io(error));
                        }
                        Err(error) => {
                            process_tree.terminate(&mut child, pid).await;
                            stop_tasks!();
                            return Err(error);
                        }
                    }
                }
                CommandEvent::Stderr(result) => {
                    let result = result.map_err(|error| {
                        GatewayError::Io(std::io::Error::other(error.to_string()))
                    });
                    match result {
                        Ok(Ok(bytes)) => stderr_result = Some(Ok(bytes)),
                        Ok(Err(StreamReadError::LimitExceeded)) => {
                            process_tree.terminate(&mut child, pid).await;
                            stop_tasks!();
                            return Err(GatewayError::CommandOutputTooLarge { bin: bin.clone() });
                        }
                        Ok(Err(StreamReadError::Io(error))) => {
                            process_tree.terminate(&mut child, pid).await;
                            stop_tasks!();
                            return Err(GatewayError::Io(error));
                        }
                        Err(error) => {
                            process_tree.terminate(&mut child, pid).await;
                            stop_tasks!();
                            return Err(error);
                        }
                    }
                }
            }
        }
        let status = child_status.expect("child status is collected before command completion");
        let stdout = stdout_result
            .expect("stdout result is collected before command completion")
            .map_err(|error| match error {
                StreamReadError::LimitExceeded => {
                    GatewayError::CommandOutputTooLarge { bin: bin.clone() }
                }
                StreamReadError::Io(error) => GatewayError::Io(error),
            })?;
        let stderr = stderr_result
            .expect("stderr result is collected before command completion")
            .map_err(|error| match error {
                StreamReadError::LimitExceeded => {
                    GatewayError::CommandOutputTooLarge { bin: bin.clone() }
                }
                StreamReadError::Io(error) => GatewayError::Io(error),
            })?;
        if let Some(stdin_task) = stdin_task {
            stdin_task
                .await
                .map_err(|error| GatewayError::Io(std::io::Error::other(error.to_string())))?
                .map_err(GatewayError::Io)?;
        }
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
            stdout_bytes: stdout,
            stderr_bytes: stderr,
        })
    }

    #[doc(hidden)]
    pub async fn kill_container(&self, runtime: &str, container_id: &str) {
        let _ = timeout(
            Duration::from_secs(10),
            Command::new(runtime)
                .args(["kill", container_id])
                .current_dir(&self.workspace)
                .env_clear()
                .env("PATH", std::env::var("PATH").unwrap_or_default())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .status(),
        )
        .await;
    }

    pub fn command_plan(&self, argv: &[String]) -> Result<Value, GatewayError> {
        let permission = self.command_permission(argv)?;
        Ok(json!({
            "argv": argv,
            "cwd": self.workspace.as_str(),
            "isolation": permission.isolation,
            "image": permission.image,
            "env_clear": true,
            "env": {
                "PATH": std::env::var("PATH").unwrap_or_default(),
                "TMPDIR": self.workspace.as_str(),
            },
            "stdin": "null",
            "timeout_seconds": self.limits.command_timeout_seconds,
            "input_limit_bytes": self.limits.command_input_limit_bytes,
            "output_limit_bytes": self.limits.command_output_limit_bytes,
            "permission": {
                "bin": &permission.bin,
                "args": &permission.args,
                "purpose": &permission.purpose,
                "isolation": &permission.isolation,
                "image": &permission.image,
            }
        }))
    }

    fn command_permission(&self, argv: &[String]) -> Result<&CommandPermission, GatewayError> {
        let (bin, args) = argv.split_first().ok_or(GatewayError::EmptyCommand)?;
        let permissions = self
            .permissions
            .commands
            .iter()
            .filter(|permission| permission.bin == *bin)
            .collect::<Vec<_>>();
        if permissions.is_empty() {
            return Err(GatewayError::CommandDenied {
                bin: bin.clone(),
                allowed: command_permission_summaries(&self.permissions.commands),
            });
        }
        permissions
            .iter()
            .copied()
            .find(|permission| args_allowed(permission, args))
            .ok_or_else(|| GatewayError::CommandArgsDenied {
                bin: bin.clone(),
                actual: args.to_vec(),
                allowed: permissions
                    .iter()
                    .map(|permission| command_permission_summary(permission))
                    .collect(),
            })
    }
}

enum StreamReadError {
    LimitExceeded,
    Io(std::io::Error),
}

async fn read_stream_bounded<R>(
    mut stream: R,
    used: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    limit: usize,
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
        let reserved = used.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(read).filter(|next| *next <= limit)
        });
        if reserved.is_err() {
            return Err(StreamReadError::LimitExceeded);
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
}

fn resolve_command_program(
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

fn container_runtime_argv(runtime: &ContainerRuntime) -> Option<Vec<String>> {
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

fn configure_process_group(command: &mut Command) {
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

struct ProcessTreeGuard {
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
}

impl ProcessTreeGuard {
    fn attach(child: &tokio::process::Child) -> Result<Self, std::io::Error> {
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

    async fn terminate(&self, child: &mut tokio::process::Child, pid: Option<u32>) {
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

fn command_permission_summaries(
    permissions: &[CommandPermission],
) -> Vec<CommandPermissionSummary> {
    permissions.iter().map(command_permission_summary).collect()
}

fn command_permission_summary(permission: &CommandPermission) -> CommandPermissionSummary {
    CommandPermissionSummary {
        bin: permission.bin.clone(),
        args: permission.args.clone(),
        purpose: permission.purpose.clone(),
        isolation: permission.isolation.clone(),
        image: permission.image.clone(),
    }
}

fn args_allowed(permission: &CommandPermission, args: &[String]) -> bool {
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

#[derive(Debug, Clone)]
pub struct HttpGateway {
    permissions: Permissions,
    client: Client,
    timeout: Duration,
    body_limit_bytes: usize,
    redirect_limit: usize,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    /// Query parameters containing credentials. They are appended only after
    /// permission checks and removed from all returned URLs and errors.
    pub sensitive_query: BTreeMap<String, String>,
    pub body: Option<Vec<u8>>,
    pub follow_redirects: bool,
}

#[derive(Debug, Clone)]
pub struct HttpOutput {
    pub status: u16,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
}

impl HttpGateway {
    pub fn new(permissions: Permissions, limits: &RuntimeLimits) -> Result<Self, GatewayError> {
        let client = Client::builder().redirect(Policy::none()).build()?;
        Ok(Self {
            permissions,
            client,
            timeout: Duration::from_secs(limits.http_timeout_seconds),
            body_limit_bytes: limits.http_body_limit_bytes,
            redirect_limit: limits.http_redirect_limit,
            cancellation: CancellationToken::new(),
        })
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub async fn request(&self, request: HttpRequest) -> Result<HttpOutput, GatewayError> {
        if request.follow_redirects && !request.sensitive_query.is_empty() {
            return Err(GatewayError::UnsupportedUrl {
                url: "requests with sensitive query parameters cannot follow redirects".into(),
            });
        }
        if request.follow_redirects
            && request
                .headers
                .keys()
                .any(|name| credential_like_name(name))
        {
            return Err(GatewayError::UnsupportedUrl {
                url: "requests with credential headers cannot follow redirects".into(),
            });
        }
        if request
            .body
            .as_ref()
            .is_some_and(|body| body.len() > self.body_limit_bytes)
        {
            return Err(GatewayError::HttpRequestBodyTooLarge {
                url: request.url.clone(),
            });
        }
        let mut url = request.url.clone();
        for _ in 0..=self.redirect_limit {
            ensure_url_allowed(&self.permissions, &url)?;
            let method =
                request
                    .method
                    .parse::<Method>()
                    .map_err(|_| GatewayError::UnsupportedUrl {
                        url: request.method.clone(),
                    })?;
            let mut request_url =
                Url::parse(&url).map_err(|_| GatewayError::UnsupportedUrl { url: url.clone() })?;
            if !request.sensitive_query.is_empty() {
                let mut pairs = request_url.query_pairs_mut();
                for (key, value) in &request.sensitive_query {
                    pairs.append_pair(key, value);
                }
            }
            let mut builder = self
                .client
                .request(method, request_url)
                .timeout(self.timeout);
            for (key, value) in &request.headers {
                builder = builder.header(key, value);
            }
            if let Some(body) = &request.body {
                builder = builder.body(body.clone());
            }
            let mut response = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                response = builder.send() => response.map_err(|error| GatewayError::Http(error.without_url()))?,
            };
            let status = response.status();
            if request.follow_redirects
                && status.is_redirection()
                && let Some(location) = response.headers().get(reqwest::header::LOCATION)
            {
                let location = location
                    .to_str()
                    .map_err(|_| GatewayError::UnsupportedUrl { url: url.clone() })?;
                url = resolve_redirect(&url, location)?;
                continue;
            }
            let final_url = response.url().to_string();
            ensure_url_allowed(&self.permissions, &final_url)?;
            let public_final_url = redact_query_parameters(&final_url, &request.sensitive_query)?;
            let headers = response
                .headers()
                .iter()
                .filter_map(|(key, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (key.as_str().to_string(), value.to_string()))
                })
                .collect();
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let initial_capacity = response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or_default()
                .min(self.body_limit_bytes);
            let mut body = Vec::with_capacity(initial_capacity);
            loop {
                let chunk = tokio::select! {
                    _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                    chunk = response.chunk() => chunk.map_err(|error| GatewayError::Http(error.without_url()))?,
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if body.len().saturating_add(chunk.len()) > self.body_limit_bytes {
                    return Err(GatewayError::HttpBodyTooLarge {
                        url: public_final_url.clone(),
                    });
                }
                body.extend_from_slice(&chunk);
            }
            return Ok(HttpOutput {
                status: status.as_u16(),
                url: public_final_url,
                headers,
                body,
                content_type,
            });
        }
        Err(GatewayError::UnsupportedUrl { url })
    }
}

fn redact_query_parameters(
    value: &str,
    sensitive: &BTreeMap<String, String>,
) -> Result<String, GatewayError> {
    if sensitive.is_empty() {
        return Ok(value.to_string());
    }
    let mut url = Url::parse(value).map_err(|_| GatewayError::UnsupportedUrl {
        url: "HTTP response returned an invalid final URL".into(),
    })?;
    let retained = url
        .query_pairs()
        .filter(|(key, _)| !sensitive.contains_key(key.as_ref()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !retained.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in retained {
            pairs.append_pair(&key, &value);
        }
    }
    Ok(url.to_string())
}

fn ensure_url_allowed(permissions: &Permissions, url: &str) -> Result<(), GatewayError> {
    let parsed = Url::parse(url).map_err(|_| GatewayError::UnsupportedUrl {
        url: url.to_string(),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(GatewayError::UnsupportedUrl {
            url: url.to_string(),
        });
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| GatewayError::UnsupportedUrl {
            url: url.to_string(),
        })?
        .to_string();
    let allowed = permissions.network.iter().any(|entry| {
        entry == "*"
            || entry == &host
            || Url::parse(entry)
                .ok()
                .and_then(|url| url.host_str().map(str::to_string))
                .as_deref()
                == Some(host.as_str())
    });
    if allowed {
        Ok(())
    } else {
        Err(GatewayError::NetworkDenied { host })
    }
}

fn resolve_redirect(base: &str, location: &str) -> Result<String, GatewayError> {
    let base = Url::parse(base).map_err(|_| GatewayError::UnsupportedUrl {
        url: base.to_string(),
    })?;
    base.join(location)
        .map(|url| url.to_string())
        .map_err(|_| GatewayError::UnsupportedUrl {
            url: location.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_workspace() -> Utf8PathBuf {
        let path = Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-gateway-test"))
            .expect("temporary directory path must be utf-8");
        std::fs::create_dir_all(&path).expect("test workspace should be created");
        path
    }

    #[test]
    fn denies_workspace_write_without_permission() {
        let mut permissions = Permissions::default();
        permissions.fs_write.clear();
        let gateway = FsGateway::new(temp_workspace(), &permissions);
        assert!(matches!(
            gateway.resolve_write("out.txt"),
            Err(GatewayError::FsWriteDenied)
        ));
    }

    #[test]
    fn denies_path_escape_even_with_workspace_permission() {
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(temp_workspace(), &permissions);
        assert!(matches!(
            gateway.resolve_write("../out.txt"),
            Err(GatewayError::PathDenied { .. })
        ));
    }

    #[test]
    fn denies_read_without_read_permission() {
        let mut permissions = Permissions::default();
        permissions.fs_read.clear();
        let gateway = FsGateway::new(temp_workspace(), &permissions);
        assert!(matches!(
            gateway.resolve_read("out.txt"),
            Err(GatewayError::FsReadDenied)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn denies_symlink_read_escape() {
        let workspace = temp_workspace();
        let outside = workspace
            .parent()
            .expect("test workspace has parent")
            .join("qcg-gateway-outside.txt");
        std::fs::write(&outside, "outside").expect("outside file should be written");
        let link = workspace.join("outside-link");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&outside, &link).expect("symlink should be created");
        let mut permissions = Permissions::default();
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace, &permissions);
        assert!(matches!(
            gateway.resolve_read("outside-link"),
            Err(GatewayError::PathDenied { .. })
        ));
    }

    #[test]
    fn permits_declared_command_shape() {
        let permission = CommandPermission {
            bin: "cc".into(),
            args: vec!["-o".into(), "*".into(), "*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        let args = vec!["-o".into(), "hello".into(), "main.c".into()];
        assert!(args_allowed(&permission, &args));
        let denied = vec!["-shared".into(), "main.c".into()];
        assert!(!args_allowed(&permission, &denied));
    }

    #[test]
    fn command_plan_uses_the_same_runtime_limits_as_execution() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "date".into(),
            args: vec![],
            purpose: "show time".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let limits = RuntimeLimits {
            command_timeout_seconds: 17,
            command_output_limit_bytes: 4096,
            ..RuntimeLimits::default()
        };
        let plan = CmdGateway::new(permissions, temp_workspace())
            .with_limits(limits)
            .command_plan(&["date".into()])
            .expect("declared command should have a plan");
        assert_eq!(plan["timeout_seconds"], 17);
        assert_eq!(plan["output_limit_bytes"], 4096);
    }

    #[tokio::test]
    async fn workspace_relative_command_resolves_against_the_declared_workspace() {
        let workspace = temp_workspace().join(format!(
            "relative-command-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let executable_name = if cfg!(windows) {
            "local-command.exe"
        } else {
            "local-command"
        };
        let executable = workspace.join(executable_name);
        std::fs::copy(
            std::env::current_exe().expect("current test executable should resolve"),
            &executable,
        )
        .expect("test executable should be copied into the workspace");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))
                .expect("test executable should be executable");
        }
        let bin = format!("./{executable_name}");
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: bin.clone(),
            args: vec!["--list".into()],
            purpose: "verify workspace-relative execution".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });

        let output = CmdGateway::new(permissions, workspace.clone())
            .run_with_limits(&[bin, "--list".into()], 30, 1024 * 1024)
            .await
            .expect("workspace-relative executable should run");

        assert_eq!(output.status, 0);
        assert!(output.stdout.contains("workspace_relative_command"));
        std::fs::remove_dir_all(workspace).expect("test workspace should be removed");
    }

    #[test]
    fn workspace_relative_command_rejects_parent_traversal() {
        let error = resolve_command_program(&temp_workspace(), "./../outside")
            .expect_err("parent traversal must be rejected before filesystem resolution");
        assert!(matches!(error, GatewayError::CommandPathDenied { .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_stops_a_running_command() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "sh".into(),
            args: vec!["-c".into(), "sleep 30".into()],
            purpose: "cancellation test".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let cancellation = CancellationToken::new();
        let gateway =
            CmdGateway::new(permissions, temp_workspace()).with_cancellation(cancellation.clone());
        let task = tokio::spawn(async move {
            gateway
                .run(&["sh".into(), "-c".into(), "sleep 30".into()])
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("command cancellation should not wait for the timeout")
            .expect("command task should join");
        assert!(matches!(result, Err(GatewayError::Canceled)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_stops_stdout_overflow_during_read() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let result = gateway
            .run_trusted_process(
                &["sh".into(), "-c".into(), "head -c 4097 /dev/zero".into()],
                5,
                4096,
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_stops_stderr_overflow_during_read() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let result = gateway
            .run_trusted_process(
                &[
                    "sh".into(),
                    "-c".into(),
                    "head -c 4097 /dev/zero >&2".into(),
                ],
                5,
                4096,
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_is_shared_between_stdout_and_stderr() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let result = gateway
            .run_trusted_process(
                &[
                    "sh".into(),
                    "-c".into(),
                    "head -c 3000 /dev/zero; head -c 3000 /dev/zero >&2".into(),
                ],
                5,
                4096,
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_is_observed_after_one_stream_eof() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let started = tokio::time::Instant::now();
        let result = gateway
            .run_trusted_process(
                &[
                    "sh".into(),
                    "-c".into(),
                    "printf done; exec 1>&-; head -c 8192 /dev/zero >&2".into(),
                ],
                5,
                4096,
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn command_stdin_limit_is_separate_from_output_limit() {
        let limits = RuntimeLimits {
            command_input_limit_bytes: 4,
            command_output_limit_bytes: 4096,
            ..RuntimeLimits::default()
        };
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace()).with_limits(limits);
        let result = gateway
            .run_trusted_process_with_stdin(&["cat".into()], 5, 4096, Some(b"12345"))
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandInputTooLarge { bin }) if bin == "cat"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_environment_contains_only_explicitly_forwarded_values() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let output = gateway
            .run_trusted_process(&["env".into()], 5, 4096)
            .await
            .expect("environment inspection should run");
        assert_eq!(output.status, 0);

        let environment = output
            .stdout
            .lines()
            .filter_map(|line| line.split_once('=').map(|(name, _)| name))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            environment,
            std::collections::BTreeSet::from(["PATH", "TMPDIR"]),
            "child processes must not inherit provider credentials or other parent state"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_kills_the_command_process_group() {
        let workspace = temp_workspace().join(format!("process-group-{}", std::process::id()));
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let script = "sleep 30 & child=$!; printf '%s' \"$child\" > child.pid; wait";
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "sh".into(),
            args: vec!["-c".into(), script.into()],
            purpose: "process group cancellation test".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let cancellation = CancellationToken::new();
        let gateway =
            CmdGateway::new(permissions, workspace.clone()).with_cancellation(cancellation.clone());
        let task = tokio::spawn(async move {
            gateway
                .run(&["sh".into(), "-c".into(), script.into()])
                .await
        });
        let child_pid = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(source) = tokio::fs::read_to_string(workspace.join("child.pid")).await
                    && let Ok(pid) = source.parse::<i32>()
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("grandchild pid should be recorded");
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("process group cancellation should be prompt")
            .expect("command task should join");
        assert!(matches!(result, Err(GatewayError::Canceled)));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // SAFETY: signal 0 only probes whether the recorded process still exists.
                let exists = unsafe { libc::kill(child_pid, 0) } == 0
                    || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
                if !exists {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("grandchild process should be reaped");
    }

    #[test]
    fn wildcard_command_args_reject_path_escape_shapes() {
        let permission = CommandPermission {
            bin: "test".into(),
            args: vec!["-f".into(), "*".into()],
            purpose: "probe file".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        for denied in [
            "../secret",
            "/tmp/secret",
            r"..\secret",
            "ok/../secret",
            "bad\0arg",
        ] {
            assert!(
                !args_allowed(&permission, &["-f".into(), denied.into()]),
                "wildcard should reject {denied:?}"
            );
        }
        assert!(args_allowed(
            &permission,
            &["-f".into(), "configs/qpx.yaml".into()]
        ));
    }

    #[test]
    fn suffix_wildcard_command_args_reject_path_escape_shapes() {
        let permission = CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile source".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        assert!(args_allowed(&permission, &["main.c".into()]));
        assert!(args_allowed(&permission, &["src/main.c".into()]));
        assert!(!args_allowed(&permission, &["../main.c".into()]));
        assert!(!args_allowed(&permission, &[r"..\main.c".into()]));
    }

    #[test]
    fn command_denial_reports_allowed_declarations() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["-o".into(), "*".into(), "*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        let denied = gateway
            .command_plan(&["cc".into(), "-shared".into(), "main.c".into()])
            .expect_err("undeclared command arguments should be denied");
        match denied {
            GatewayError::CommandArgsDenied {
                bin,
                actual,
                allowed,
            } => {
                assert_eq!(bin, "cc");
                assert_eq!(actual, vec!["-shared", "main.c"]);
                assert_eq!(allowed.len(), 1);
                assert_eq!(allowed[0].purpose, "compile");
            }
            other => panic!("unexpected gateway error: {other}"),
        }
    }

    #[test]
    fn command_plan_uses_the_first_matching_permission_for_the_bin() {
        let mut permissions = Permissions::default();
        permissions.commands.extend([
            CommandPermission {
                bin: "cc".into(),
                args: vec!["-shared".into(), "*.c".into()],
                purpose: "build a shared library".into(),
                isolation: Some(CommandIsolation::TrustedHost),
                image: None,
            },
            CommandPermission {
                bin: "cc".into(),
                args: vec!["-o".into(), "*".into(), "*.c".into()],
                purpose: "build an executable".into(),
                isolation: Some(CommandIsolation::TrustedHost),
                image: None,
            },
        ]);
        let plan = CmdGateway::new(permissions, temp_workspace())
            .command_plan(&["cc".into(), "-o".into(), "app".into(), "main.c".into()])
            .expect("a later matching permission should be accepted");
        assert_eq!(plan["permission"]["purpose"], "build an executable");
    }

    #[test]
    fn denies_undeclared_network_host() {
        let permissions = Permissions::default();
        assert!(matches!(
            ensure_url_allowed(&permissions, "https://example.com/"),
            Err(GatewayError::NetworkDenied { .. })
        ));
    }

    #[test]
    fn permits_declared_network_host() {
        let mut permissions = Permissions::default();
        permissions.network.push("example.com".into());
        assert!(ensure_url_allowed(&permissions, "https://example.com/path").is_ok());
    }

    #[test]
    fn sensitive_query_parameters_are_removed_from_public_urls() {
        let sensitive = BTreeMap::from([("api_key".to_string(), "secret-value".to_string())]);
        let public = redact_query_parameters(
            "https://example.com/search?engine=google&api_key=secret-value&q=qcg",
            &sensitive,
        )
        .expect("URL should be redacted");
        assert_eq!(public, "https://example.com/search?engine=google&q=qcg");
        assert!(!public.contains("secret-value"));
    }

    macro_rules! fs_write_denied_path_case {
        ($name:ident, $path:expr) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.fs_write.push("workspace".into());
                let gateway = FsGateway::new(temp_workspace(), &permissions);
                assert!(
                    matches!(
                        gateway.resolve_write($path),
                        Err(GatewayError::PathDenied { .. })
                    ),
                    "write path should be denied: {:?}",
                    $path
                );
            }
        };
    }

    macro_rules! fs_read_denied_path_case {
        ($name:ident, $path:expr) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.fs_read.push("workspace".into());
                let gateway = FsGateway::new(temp_workspace(), &permissions);
                assert!(
                    matches!(
                        gateway.resolve_read($path),
                        Err(GatewayError::PathDenied { .. })
                    ),
                    "read path should be denied: {:?}",
                    $path
                );
            }
        };
    }

    macro_rules! wildcard_arg_denied_case {
        ($name:ident, $pattern:expr, $actual:expr) => {
            #[test]
            fn $name() {
                let permission = CommandPermission {
                    bin: "tool".into(),
                    args: vec![$pattern.into()],
                    purpose: "test wildcard".into(),
                    isolation: Some(CommandIsolation::TrustedHost),
                    image: None,
                };
                assert!(
                    !args_allowed(&permission, &[$actual.into()]),
                    "pattern {:?} should deny {:?}",
                    $pattern,
                    $actual
                );
            }
        };
    }

    macro_rules! wildcard_arg_allowed_case {
        ($name:ident, $pattern:expr, $actual:expr) => {
            #[test]
            fn $name() {
                let permission = CommandPermission {
                    bin: "tool".into(),
                    args: vec![$pattern.into()],
                    purpose: "test wildcard".into(),
                    isolation: Some(CommandIsolation::TrustedHost),
                    image: None,
                };
                assert!(
                    args_allowed(&permission, &[$actual.into()]),
                    "pattern {:?} should allow {:?}",
                    $pattern,
                    $actual
                );
            }
        };
    }

    macro_rules! url_allowed_case {
        ($name:ident, $allow:expr, $url:expr) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.network.push($allow.into());
                assert!(
                    ensure_url_allowed(&permissions, $url).is_ok(),
                    "allow {:?} should permit {:?}",
                    $allow,
                    $url
                );
            }
        };
    }

    macro_rules! url_denied_case {
        ($name:ident, $allow:expr, $url:expr, $error:pat) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.network.push($allow.into());
                assert!(
                    matches!(ensure_url_allowed(&permissions, $url), Err($error)),
                    "allow {:?} should deny {:?}",
                    $allow,
                    $url
                );
            }
        };
    }

    fs_write_denied_path_case!(fs_write_denies_parent_escape, "../secret.txt");
    fs_write_denied_path_case!(fs_write_denies_nested_parent_escape, "a/../../secret.txt");
    fs_write_denied_path_case!(fs_write_denies_absolute_unix_path, "/tmp/secret.txt");
    fs_write_denied_path_case!(fs_write_denies_backslash_parent_escape, r"..\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_backslash_separator, r"dir\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_nul_byte, "dir\0secret.txt");
    fs_write_denied_path_case!(fs_write_denies_current_dir_component, "./secret.txt");
    fs_write_denied_path_case!(
        fs_write_denies_current_dir_then_parent_dir,
        "dir/./../secret.txt"
    );
    fs_write_denied_path_case!(fs_write_denies_embedded_parent_dir, "dir/../secret.txt");
    fs_write_denied_path_case!(fs_write_denies_parent_only, "..");
    fs_write_denied_path_case!(fs_write_denies_dir_parent_suffix, "dir/..");
    fs_write_denied_path_case!(fs_write_denies_trailing_parent, "dir/sub/..");
    fs_write_denied_path_case!(
        fs_write_denies_deep_parent_escape,
        "dir/sub/../../secret.txt"
    );
    fs_write_denied_path_case!(fs_write_denies_unc_shape, r"\\server\share\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_windows_device_shape, r"\\?\C:\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_windows_drive_backslash, r"C:\secret.txt");
    fs_write_denied_path_case!(
        fs_write_denies_forward_then_backslash,
        r"dir/sub\secret.txt"
    );
    fs_write_denied_path_case!(
        fs_write_denies_parent_after_normalized_leaf,
        "dir/.../../secret.txt"
    );
    fs_write_denied_path_case!(
        fs_write_denies_repeated_parent_components,
        "../../secret.txt"
    );
    fs_write_denied_path_case!(fs_write_denies_dot_parent_combo, "./../secret.txt");

    fs_read_denied_path_case!(fs_read_denies_parent_escape, "../secret.txt");
    fs_read_denied_path_case!(fs_read_denies_nested_parent_escape, "a/../../secret.txt");
    fs_read_denied_path_case!(fs_read_denies_absolute_unix_path, "/tmp/secret.txt");
    fs_read_denied_path_case!(fs_read_denies_backslash_parent_escape, r"..\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_backslash_separator, r"dir\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_nul_byte, "dir\0secret.txt");
    fs_read_denied_path_case!(fs_read_denies_current_dir_component, "./secret.txt");
    fs_read_denied_path_case!(
        fs_read_denies_current_dir_then_parent_dir,
        "dir/./../secret.txt"
    );
    fs_read_denied_path_case!(fs_read_denies_embedded_parent_dir, "dir/../secret.txt");
    fs_read_denied_path_case!(fs_read_denies_parent_only, "..");
    fs_read_denied_path_case!(fs_read_denies_dir_parent_suffix, "dir/..");
    fs_read_denied_path_case!(fs_read_denies_trailing_parent, "dir/sub/..");
    fs_read_denied_path_case!(
        fs_read_denies_deep_parent_escape,
        "dir/sub/../../secret.txt"
    );
    fs_read_denied_path_case!(fs_read_denies_unc_shape, r"\\server\share\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_windows_device_shape, r"\\?\C:\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_windows_drive_backslash, r"C:\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_forward_then_backslash, r"dir/sub\secret.txt");
    fs_read_denied_path_case!(
        fs_read_denies_parent_after_normalized_leaf,
        "dir/.../../secret.txt"
    );
    fs_read_denied_path_case!(
        fs_read_denies_repeated_parent_components,
        "../../secret.txt"
    );
    fs_read_denied_path_case!(fs_read_denies_dot_parent_combo, "./../secret.txt");

    wildcard_arg_denied_case!(wildcard_star_denies_empty_arg, "*", "");
    wildcard_arg_denied_case!(wildcard_star_denies_parent_escape, "*", "../secret");
    wildcard_arg_denied_case!(
        wildcard_star_denies_nested_parent_escape,
        "*",
        "ok/../secret"
    );
    wildcard_arg_denied_case!(wildcard_star_denies_absolute_path, "*", "/tmp/secret");
    wildcard_arg_denied_case!(wildcard_star_denies_backslash_parent, "*", r"..\secret");
    wildcard_arg_denied_case!(wildcard_star_denies_backslash_separator, "*", r"dir\secret");
    wildcard_arg_denied_case!(wildcard_star_denies_nul_byte, "*", "bad\0arg");
    wildcard_arg_denied_case!(wildcard_suffix_denies_parent_escape, "*.c", "../main.c");
    wildcard_arg_denied_case!(
        wildcard_suffix_denies_nested_parent_escape,
        "*.c",
        "src/../main.c"
    );
    wildcard_arg_denied_case!(wildcard_suffix_denies_absolute_path, "*.c", "/tmp/main.c");
    wildcard_arg_denied_case!(wildcard_suffix_denies_backslash_parent, "*.c", r"..\main.c");
    wildcard_arg_denied_case!(
        wildcard_suffix_denies_backslash_separator,
        "*.c",
        r"src\main.c"
    );
    wildcard_arg_denied_case!(wildcard_suffix_denies_nul_byte, "*.c", "main\0.c");
    wildcard_arg_denied_case!(wildcard_prefix_denies_parent_escape, "src/*", "../main.c");
    wildcard_arg_denied_case!(
        wildcard_prefix_denies_nested_parent_escape,
        "src/*",
        "src/../main.c"
    );
    wildcard_arg_denied_case!(wildcard_prefix_denies_absolute_path, "src/*", "/tmp/main.c");
    wildcard_arg_denied_case!(
        wildcard_prefix_denies_backslash_separator,
        "src/*",
        r"src\main.c"
    );
    wildcard_arg_denied_case!(wildcard_prefix_denies_nul_byte, "src/*", "src/main\0.c");
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_parent_escape,
        "--file=*",
        "--file=../secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_empty_value,
        "--file=*",
        "--file="
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_absolute_path,
        "--file=*",
        "--file=/tmp/secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_nested_parent_escape,
        "--file=*",
        "--file=ok/../secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_backslash,
        "--file=*",
        r"--file=..\secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_nul_byte,
        "--file=*",
        "--file=bad\0arg"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_parent_escape,
        "*-out",
        "../build-out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_nested_parent,
        "*-out",
        "build/../out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_absolute_path,
        "*-out",
        "/tmp/build-out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_backslash,
        "*-out",
        r"build\app-out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_nul_byte,
        "*-out",
        "build\0-out"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_parent_escape,
        "*.json",
        "../data.json"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_nested_parent,
        "*.json",
        "data/../data.json"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_absolute_path,
        "*.json",
        "/tmp/data.json"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_backslash,
        "*.json",
        r"data\data.json"
    );

    wildcard_arg_allowed_case!(wildcard_star_allows_filename, "*", "config.yaml");
    wildcard_arg_allowed_case!(
        wildcard_star_allows_nested_relative_path,
        "*",
        "configs/qpx.yaml"
    );
    wildcard_arg_allowed_case!(wildcard_suffix_allows_leaf_c_file, "*.c", "main.c");
    wildcard_arg_allowed_case!(wildcard_suffix_allows_nested_c_file, "*.c", "src/main.c");
    wildcard_arg_allowed_case!(wildcard_prefix_allows_nested_file, "src/*", "src/main.c");
    wildcard_arg_allowed_case!(
        wildcard_prefix_allows_deep_nested_file,
        "src/*",
        "src/bin/main.c"
    );
    wildcard_arg_allowed_case!(
        wildcard_flag_value_allows_relative_file,
        "--file=*",
        "--file=config.yaml"
    );
    wildcard_arg_allowed_case!(wildcard_output_suffix_allows_leaf, "*-out", "build-out");
    wildcard_arg_allowed_case!(wildcard_json_suffix_allows_leaf, "*.json", "data.json");
    wildcard_arg_allowed_case!(
        wildcard_json_suffix_allows_nested_file,
        "*.json",
        "data/input.json"
    );
    wildcard_arg_allowed_case!(
        wildcard_toml_suffix_allows_nested_file,
        "*.toml",
        "generators/qcg.toml"
    );
    wildcard_arg_allowed_case!(
        wildcard_log_suffix_allows_nested_file,
        "*.log",
        "runs/latest.log"
    );
    wildcard_arg_allowed_case!(
        wildcard_prefix_allows_dash_filename,
        "src/*",
        "src/my-file.rs"
    );
    wildcard_arg_allowed_case!(wildcard_star_allows_dash_filename, "*", "my-file.rs");
    wildcard_arg_allowed_case!(wildcard_star_allows_dot_filename, "*", "qcg.toml");

    url_allowed_case!(
        url_allows_exact_host_https,
        "example.com",
        "https://example.com/path"
    );
    url_allowed_case!(
        url_allows_exact_host_http,
        "example.com",
        "http://example.com/path"
    );
    url_allowed_case!(
        url_allows_full_url_host_https,
        "https://example.com/base",
        "https://example.com/other"
    );
    url_allowed_case!(
        url_allows_full_url_host_http,
        "http://example.com/base",
        "http://example.com/other"
    );
    url_allowed_case!(url_allows_wildcard_https, "*", "https://example.net/path");
    url_allowed_case!(url_allows_wildcard_http, "*", "http://example.net/path");
    url_allowed_case!(
        url_allows_host_with_port_by_host,
        "127.0.0.1",
        "http://127.0.0.1:8080/path"
    );
    url_allowed_case!(
        url_allows_localhost_with_port_by_host,
        "localhost",
        "http://localhost:3000/path"
    );
    url_allowed_case!(
        url_allows_subdomain_when_exact,
        "api.example.com",
        "https://api.example.com/v1"
    );
    url_allowed_case!(
        url_allows_case_normalized_host,
        "example.com",
        "https://EXAMPLE.com/path"
    );

    url_denied_case!(
        url_denies_different_host,
        "example.com",
        "https://other.example.com/path",
        GatewayError::NetworkDenied { .. }
    );
    url_denied_case!(
        url_denies_parent_domain,
        "api.example.com",
        "https://example.com/path",
        GatewayError::NetworkDenied { .. }
    );
    url_denied_case!(
        url_denies_sibling_subdomain,
        "api.example.com",
        "https://cdn.example.com/path",
        GatewayError::NetworkDenied { .. }
    );
    url_denied_case!(
        url_denies_file_scheme,
        "*",
        "file:///tmp/secret",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_ftp_scheme,
        "*",
        "ftp://example.com/file",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_missing_host,
        "*",
        "https://",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_relative_url,
        "*",
        "/relative/path",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_malformed_url,
        "*",
        "not a url",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_javascript_scheme,
        "*",
        "javascript:alert(1)",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_data_scheme,
        "*",
        "data:text/plain,secret",
        GatewayError::UnsupportedUrl { .. }
    );

    #[test]
    fn command_plan_denies_empty_argv() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        assert!(matches!(
            gateway.command_plan(&[]),
            Err(GatewayError::EmptyCommand)
        ));
    }

    #[test]
    fn command_plan_denies_undeclared_bin_with_allowed_summary() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        let error = gateway
            .command_plan(&["sh".into(), "-c".into(), "echo no".into()])
            .expect_err("undeclared bin should be denied");
        match error {
            GatewayError::CommandDenied { bin, allowed } => {
                assert_eq!(bin, "sh");
                assert_eq!(allowed.len(), 1);
                assert_eq!(allowed[0].bin, "cc");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn command_plan_denies_extra_args_for_empty_pattern() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "date".into(),
            args: vec![],
            purpose: "print date".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["date".into(), "-u".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }

    #[test]
    fn command_plan_denies_missing_arg_for_declared_pattern() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["cc".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }

    #[test]
    fn command_plan_denies_too_many_args_for_declared_pattern() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["cc".into(), "main.c".into(), "extra.c".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }
}
