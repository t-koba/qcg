use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{CommandPermission, Permissions, RuntimeLimits};
use reqwest::{Client, Method, redirect::Policy};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
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
    #[error("command `{bin}` timed out")]
    CommandTimedOut { bin: String },
    #[error("command `{bin}` output exceeded limit")]
    CommandOutputTooLarge { bin: String },
    #[error("network access to host `{host}` is not allowed by permissions.network")]
    NetworkDenied { host: String },
    #[error("unsupported URL `{url}`")]
    UnsupportedUrl { url: String },
    #[error("HTTP response body exceeded limit for `{url}`")]
    HttpBodyTooLarge { url: String },
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
        self.command_plan(argv)?;
        self.run_trusted_process(
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
        self.command_plan(argv)?;
        self.run_trusted_process(argv, timeout_seconds, output_limit_bytes)
            .await
    }

    #[doc(hidden)]
    pub async fn run_trusted_process(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: usize,
    ) -> Result<CommandOutput, GatewayError> {
        let (bin, args) = argv.split_first().ok_or(GatewayError::EmptyCommand)?;
        let mut command = Command::new(bin);
        command
            .args(args)
            .current_dir(&self.workspace)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("TMPDIR", self.workspace.as_str())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        configure_process_group(&mut command);
        let mut child = command.spawn()?;
        let process_tree = ProcessTreeGuard::attach(&child)?;
        let pid = child.id();
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("command stdout pipe was not available"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| std::io::Error::other("command stderr pipe was not available"))?;
        let stdout_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            stdout.read_to_end(&mut bytes).await.map(|_| bytes)
        });
        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).await.map(|_| bytes)
        });
        let status = tokio::select! {
            _ = self.cancellation.cancelled() => {
                process_tree.terminate(&mut child, pid).await;
                return Err(GatewayError::Canceled);
            },
            result = timeout(Duration::from_secs(timeout_seconds), child.wait()) => {
                match result {
                    Ok(status) => status?,
                    Err(_) => {
                        process_tree.terminate(&mut child, pid).await;
                        return Err(GatewayError::CommandTimedOut { bin: bin.clone() });
                    }
                }
            }
        };
        let stdout = stdout_task.await.map_err(std::io::Error::other)??;
        let stderr = stderr_task.await.map_err(std::io::Error::other)??;
        if stdout.len() + stderr.len() > output_limit_bytes {
            return Err(GatewayError::CommandOutputTooLarge { bin: bin.clone() });
        }
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
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
        let (bin, args) = argv.split_first().ok_or(GatewayError::EmptyCommand)?;
        let permissions = self
            .permissions
            .commands
            .iter()
            .filter(|permission| permission.bin == *bin)
            .cloned()
            .collect::<Vec<_>>();
        if permissions.is_empty() {
            return Err(GatewayError::CommandDenied {
                bin: bin.clone(),
                allowed: command_permission_summaries(&self.permissions.commands),
            });
        }
        let permission = permissions
            .iter()
            .find(|permission| args_allowed(permission, args))
            .ok_or_else(|| GatewayError::CommandArgsDenied {
                bin: bin.clone(),
                actual: args.to_vec(),
                allowed: command_permission_summaries(&permissions),
            })?;
        Ok(json!({
            "argv": argv,
            "cwd": self.workspace.as_str(),
            "env_clear": true,
            "env": {
                "PATH": std::env::var("PATH").unwrap_or_default(),
                "TMPDIR": self.workspace.as_str(),
            },
            "stdin": "null",
            "timeout_seconds": self.limits.command_timeout_seconds,
            "output_limit_bytes": self.limits.command_output_limit_bytes,
            "permission": {
                "bin": &permission.bin,
                "args": &permission.args,
                "purpose": &permission.purpose,
            }
        }))
    }
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
    job: windows_sys::Win32::Foundation::HANDLE,
}

impl ProcessTreeGuard {
    fn attach(child: &tokio::process::Child) -> Result<Self, std::io::Error> {
        #[cfg(windows)]
        {
            use std::ffi::c_void;
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Foundation::CloseHandle;
            use windows_sys::Win32::System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            };

            // SAFETY: all handles and pointers are valid for the duration of each Win32 call.
            unsafe {
                let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if job.is_null() {
                    return Err(std::io::Error::last_os_error());
                }
                let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
                information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                let configured = SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    (&raw const information).cast::<c_void>(),
                    u32::try_from(std::mem::size_of_val(&information)).map_err(|_| {
                        std::io::Error::other("Windows Job Object information is too large")
                    })?,
                );
                if configured == 0 {
                    let error = std::io::Error::last_os_error();
                    CloseHandle(job);
                    return Err(error);
                }
                let process = child.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
                if AssignProcessToJobObject(job, process) == 0 {
                    let error = std::io::Error::last_os_error();
                    CloseHandle(job);
                    return Err(error);
                }
                return Ok(Self { job });
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
        if let Some(pid) = pid {
            if let Ok(pid) = i32::try_from(pid) {
                // SAFETY: the child was spawned into a new process group whose id is its pid.
                unsafe {
                    libc::killpg(pid, libc::SIGKILL);
                }
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::TerminateJobObject;
            // SAFETY: the Job Object handle remains owned by this guard.
            unsafe {
                TerminateJobObject(self.job, 1);
            }
            let _ = pid;
        }
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

#[cfg(windows)]
impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        // SAFETY: the handle is owned by this guard and is closed exactly once.
        unsafe {
            CloseHandle(self.job);
        }
    }
}

fn command_permission_summaries(
    permissions: &[CommandPermission],
) -> Vec<CommandPermissionSummary> {
    permissions
        .iter()
        .map(|permission| CommandPermissionSummary {
            bin: permission.bin.clone(),
            args: permission.args.clone(),
            purpose: permission.purpose.clone(),
        })
        .collect()
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
    pub body: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HttpOutput {
    pub status: u16,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: String,
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
            let mut builder = self.client.request(method, &url).timeout(self.timeout);
            for (key, value) in &request.headers {
                builder = builder.header(key, value);
            }
            if let Some(body) = &request.body {
                builder = builder.body(body.clone());
            }
            let response = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                response = builder.send() => response?,
            };
            let status = response.status();
            if status.is_redirection()
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
            let body = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                body = response.text() => body?,
            };
            if body.len() > self.body_limit_bytes {
                return Err(GatewayError::HttpBodyTooLarge { url: final_url });
            }
            return Ok(HttpOutput {
                status: status.as_u16(),
                url: final_url,
                headers,
                body,
            });
        }
        Err(GatewayError::UnsupportedUrl { url })
    }
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

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_stops_a_running_command() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "sh".into(),
            args: vec!["-c".into(), "sleep 30".into()],
            purpose: "cancellation test".into(),
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
            },
            CommandPermission {
                bin: "cc".into(),
                args: vec!["-o".into(), "*".into(), "*.c".into()],
                purpose: "build an executable".into(),
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
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["cc".into(), "main.c".into(), "extra.c".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }
}
