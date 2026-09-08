use camino::Utf8PathBuf;
use qcg_contract::{CommandIsolation, CommandPermission, Permissions};
use serde_json::{Value, json};
use std::process::Stdio;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use super::error::GatewayError;
use super::process::{
    ProcessTreeGuard, StreamReadError, args_allowed, command_permission_summaries,
    command_permission_summary, configure_process_group, read_stream_bounded,
    resolve_command_program,
};

/// Resolved command execution bounds. The contract supplies the values;
/// the gateway enforces them without knowing `RuntimeLimits`.
#[derive(Debug, Clone, Copy)]
pub struct CommandBounds {
    pub timeout_seconds: u64,
    pub input_limit_bytes: Option<usize>,
    pub output_limit_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct CmdGateway {
    permissions: Permissions,
    bounds: CommandBounds,
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
            bounds: CommandBounds {
                timeout_seconds: 30,
                input_limit_bytes: None,
                output_limit_bytes: None,
            },
            workspace,
            cancellation: CancellationToken::new(),
        }
    }

    pub fn with_bounds(mut self, bounds: CommandBounds) -> Self {
        self.bounds = bounds;
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub async fn run(&self, argv: &[String]) -> Result<CommandOutput, GatewayError> {
        self.run_with_limits(
            argv,
            self.bounds.timeout_seconds,
            self.bounds.output_limit_bytes,
        )
        .await
    }

    pub async fn run_with_limits(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: Option<usize>,
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
            self.bounds.timeout_seconds,
            self.bounds.output_limit_bytes,
            Some(stdin),
        )
        .await
    }

    pub async fn run_with_limits_and_stdin(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: Option<usize>,
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
        output_limit_bytes: Option<usize>,
        stdin: Option<&[u8]>,
    ) -> Result<CommandOutput, GatewayError> {
        let mounts = vec![(self.workspace.clone(), "/work".to_string(), false)];
        self.run_container_workload(
            ContainerWorkload {
                image,
                mounts: &mounts,
                workdir: Some("/work"),
                workload_argv: argv,
                stdin,
            },
            timeout_seconds,
            output_limit_bytes,
        )
        .await
    }

    /// Runs one workload inside the declared container backend. Docker-family
    /// backends execute one-shot `run`; Incus-like and legacy LXC backends
    /// provision a managed instance first. Every path owns a session guard,
    /// so cancel/timeout never orphans the daemon-side container.
    pub async fn run_container_workload(
        &self,
        spec: ContainerWorkload<'_>,
        timeout_seconds: u64,
        output_limit_bytes: Option<usize>,
    ) -> Result<CommandOutput, GatewayError> {
        use qcg_container::{Backend, Mount, Provision, SessionGuard, provision, teardown};
        let bin = spec
            .workload_argv
            .first()
            .cloned()
            .ok_or(GatewayError::EmptyCommand)?;
        let declared = self
            .permissions
            .containers
            .runtime
            .as_ref()
            .ok_or_else(|| GatewayError::ContainerRuntimeMissing { bin: bin.clone() })?;
        let backend = qcg_container::resolve_backend(declared)
            .ok_or_else(|| GatewayError::ContainerRuntimeMissing { bin: bin.clone() })?;
        let mounts: Vec<Mount<'_>> = spec
            .mounts
            .iter()
            .map(|(host, guest, readonly)| Mount {
                host: host.as_std_path(),
                guest,
                readonly: *readonly,
            })
            .collect();
        match backend {
            Backend::Docker {
                ref binary,
                ref runtime_flag,
            } => {
                // Tracking file lives outside the mounted workspace so the
                // container cannot tamper with the tracked container id.
                let cidfile = std::env::temp_dir().join(format!(
                    ".qcg-container-{}.cid",
                    uuid::Uuid::now_v7().as_simple()
                ));
                let container_argv =
                    qcg_container::docker_run_argv(&qcg_container::DockerRunSpec {
                        binary,
                        runtime_flag: runtime_flag.as_deref(),
                        cidfile: cidfile.as_path(),
                        mounts: &mounts,
                        workdir: spec.workdir,
                        env_names: &[],
                        image: spec.image,
                        workload: spec.workload_argv,
                        stdin_pipe: spec.stdin.is_some(),
                    });
                let session = qcg_container::Session {
                    backend: backend.clone(),
                    id: qcg_container::InstanceId::CidFile(cidfile),
                };
                let guard = SessionGuard::new(session.clone());
                let result = self
                    .run_trusted_process_with_stdin(
                        &container_argv,
                        timeout_seconds,
                        output_limit_bytes,
                        spec.stdin,
                    )
                    .await;
                // Awaited cleanup on every exit path; `--rm` normally removes
                // the container, but a client-side timeout leaves the daemon
                // side running without this.
                teardown(&session).await;
                guard.disarm();
                result
            }
            managed => {
                let provisioned = provision(
                    &managed,
                    &Provision {
                        image: spec.image,
                        mounts: &mounts,
                        id_prefix: "qcg-container",
                        cancel: &self.cancellation,
                    },
                )
                .await?;
                let instance = match &provisioned.id {
                    qcg_container::InstanceId::Name(name) => name.clone(),
                    qcg_container::InstanceId::CidFile(_) => {
                        return Err(GatewayError::Container(
                            qcg_container::ContainerError::StageFailed {
                                stage: "provision",
                                instance: "unknown".into(),
                                detail: "managed provision returned no instance name".into(),
                            },
                        ));
                    }
                };
                let guard = SessionGuard::new(provisioned.clone());
                let workdir = spec.workdir.or_else(|| {
                    mounts
                        .first()
                        .map(|mount| mount.guest)
                        .filter(|guest| !guest.is_empty())
                });
                let container_argv = match &managed {
                    Backend::Incus { binary } => qcg_container::incus_exec_argv(
                        binary,
                        &instance,
                        workdir,
                        &[],
                        spec.workload_argv,
                    ),
                    Backend::Lxc => {
                        // `lxc-attach` has no workdir flag; the wrapper below
                        // preserves the workload PID, stdio, signals, and
                        // exit status while entering the workdir.
                        let guest = workdir.unwrap_or("/");
                        qcg_container::lxc_exec_argv(
                            &instance,
                            qcg_container::LXC_MINIMAL_PATH,
                            guest,
                            spec.workload_argv,
                        )
                    }
                    Backend::Docker { .. } => {
                        return Err(GatewayError::Container(
                            qcg_container::ContainerError::StageFailed {
                                stage: "argv",
                                instance: instance.clone(),
                                detail: "docker backends take the one-shot path".into(),
                            },
                        ));
                    }
                };
                let result = self
                    .run_trusted_process_with_stdin(
                        &container_argv,
                        timeout_seconds,
                        output_limit_bytes,
                        spec.stdin,
                    )
                    .await;
                teardown(&provisioned).await;
                guard.disarm();
                result
            }
        }
    }

    #[doc(hidden)]
    pub async fn run_trusted_process(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: Option<usize>,
    ) -> Result<CommandOutput, GatewayError> {
        self.run_trusted_process_with_stdin(argv, timeout_seconds, output_limit_bytes, None)
            .await
    }

    #[doc(hidden)]
    pub async fn run_trusted_process_with_stdin(
        &self,
        argv: &[String],
        timeout_seconds: u64,
        output_limit_bytes: Option<usize>,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<CommandOutput, GatewayError> {
        let (bin, args) = argv.split_first().ok_or(GatewayError::EmptyCommand)?;
        if self
            .bounds
            .input_limit_bytes
            .is_some_and(|limit| stdin_bytes.is_some_and(|bytes| bytes.len() > limit))
        {
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
        let mut process_tree = ProcessTreeGuard::attach(&child)?;
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
        // Clean wait: disarm the group-kill Drop so a recycled process group
        // id is never signaled after success.
        process_tree.disarm();
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
            stdout_bytes: stdout,
            stderr_bytes: stderr,
        })
    }
}

/// One-shot container workload dispatched across every backend family.
pub struct ContainerWorkload<'a> {
    pub image: &'a str,
    /// `(host path, guest destination, readonly)` bind mounts.
    pub mounts: &'a [(Utf8PathBuf, String, bool)],
    /// Guest working directory. Docker passes `--workdir`; Incus passes
    /// `--cwd`; legacy LXC enters it through the shell wrapper.
    pub workdir: Option<&'a str>,
    pub workload_argv: &'a [String],
    pub stdin: Option<&'a [u8]>,
}

impl CmdGateway {
    pub fn command_plan(&self, argv: &[String]) -> Result<Value, GatewayError> {
        let permission = self.command_permission(argv)?;
        let runtime: Option<String> = self
            .permissions
            .containers
            .runtime
            .as_ref()
            .map(|runtime| runtime.display_name().to_string());
        Ok(json!({
            "argv": argv,
            "cwd": self.workspace.as_str(),
            "isolation": permission.isolation,
            "image": permission.image,
            "runtime": runtime,
            "env_clear": true,
            "env": {
                "PATH": std::env::var("PATH").unwrap_or_default(),
                "TMPDIR": self.workspace.as_str(),
            },
            "stdin": "null",
            "timeout_seconds": self.bounds.timeout_seconds,
            "input_limit_bytes": self.bounds.input_limit_bytes,
            "output_limit_bytes": self.bounds.output_limit_bytes,
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
