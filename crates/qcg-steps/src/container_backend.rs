use super::common::{bounded_file_bytes, bounded_sha256_file};
use qcg_contract::{ContainerRuntime, NodeDef, ToolBackendKind, ToolDef, ToolWorkspace};
use qcg_engine::{StepContext, StepError};
use serde_json::json;
use sha2::{Digest, Sha256};
pub(crate) struct ToolBackendCandidate {
    pub(crate) kind: ToolBackendKind,
    pub(crate) argv: Vec<String>,
    pub(crate) container_image: Option<String>,
    pub(crate) container_mounts: Vec<ContainerMountSpec>,
}

#[derive(Debug, Clone)]
pub(crate) struct ContainerMountSpec {
    pub(crate) target: String,
    pub(crate) mode: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ContainerOutput {
    pub(crate) runtime: String,
    pub(crate) status: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

pub(crate) fn build_tool_backend_candidate(
    ctx: &StepContext<'_>,
    _node: &NodeDef,
    tool: &ToolDef,
    backend: &ToolBackendKind,
    input: &str,
) -> Result<ToolBackendCandidate, String> {
    match backend {
        ToolBackendKind::Bundled => {
            let backend = tool
                .backends
                .bundled
                .as_ref()
                .ok_or_else(|| "bundled backend is not declared".to_string())?;
            let bin = ctx
                .run
                .templates
                .render_inline(
                    &backend.bin,
                    json!({
                        "os": current_os(),
                        "arch": current_arch(),
                    }),
                    &ctx.run.contract.manifest.runtime,
                )
                .map_err(|error| format!("failed to render bundled bin: {error}"))?;
            let path = ctx
                .run
                .contract
                .resolve_package_path(&bin)
                .map_err(|error| error.to_string())?;
            if !path.is_file() {
                return Err(format!("bundled binary `{bin}` was not found"));
            }
            let sha256 = bounded_sha256_file(
                &path,
                ctx.run.contract.manifest.runtime.file_input_limit_bytes,
            )?;
            if sha256 != backend.sha256 {
                return Err(format!(
                    "bundled binary `{bin}` sha256 mismatch: expected {}, got {sha256}",
                    backend.sha256
                ));
            }
            let argv = tool_argv(path.as_str(), &tool.command, input);
            Ok(ToolBackendCandidate {
                kind: ToolBackendKind::Bundled,
                argv,
                container_image: None,
                container_mounts: vec![],
            })
        }
        ToolBackendKind::Container => {
            let backend = tool
                .backends
                .container
                .as_ref()
                .ok_or_else(|| "container backend is not declared".to_string())?;
            if container_runtime_command(&ctx.run.contract.manifest.permissions.containers)
                .is_none()
            {
                return Err("container runtime was not found".into());
            }
            let mounted_input = if matches!(tool.workspace, ToolWorkspace::None) {
                input.to_string()
            } else {
                format!("{}/{}", backend.mount.trim_end_matches('/'), input)
            };
            let argv = tool_argv(
                tool.command.first().map(String::as_str).unwrap_or_default(),
                &tool.command,
                &mounted_input,
            );
            let container_mounts = if matches!(tool.workspace, ToolWorkspace::None) {
                vec![]
            } else {
                vec![ContainerMountSpec {
                    target: backend.mount.clone(),
                    mode: match tool.workspace {
                        ToolWorkspace::Writable => "rw".into(),
                        ToolWorkspace::ReadOnly | ToolWorkspace::None => "ro".into(),
                    },
                }]
            };
            Ok(ToolBackendCandidate {
                kind: ToolBackendKind::Container,
                argv,
                container_image: Some(backend.image.clone()),
                container_mounts,
            })
        }
        ToolBackendKind::Host => {
            let backend = tool
                .backends
                .host
                .as_ref()
                .ok_or_else(|| "host backend is not declared".to_string())?;
            if !binary_available(&backend.bin) {
                return Err(format!(
                    "host binary `{}` was not found on PATH",
                    backend.bin
                ));
            }
            let argv = tool_argv(&backend.bin, &tool.command, input);
            Ok(ToolBackendCandidate {
                kind: ToolBackendKind::Host,
                argv,
                container_image: None,
                container_mounts: vec![],
            })
        }
    }
}

pub(crate) fn tool_argv(bin: &str, command: &[String], input: &str) -> Vec<String> {
    let mut argv = vec![bin.to_string()];
    argv.extend(
        command
            .iter()
            .skip(1)
            .map(|arg| render_tool_arg(arg, input)),
    );
    argv
}

pub(crate) fn render_tool_arg(arg: &str, input: &str) -> String {
    arg.replace("{input}", input)
}

pub(crate) fn current_os() -> &'static str {
    std::env::consts::OS
}

pub(crate) fn current_arch() -> &'static str {
    std::env::consts::ARCH
}

pub(crate) fn binary_available(bin: &str) -> bool {
    if bin.contains('/') || bin.contains('\\') {
        return executable_file_exists(std::path::Path::new(bin));
    }
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| executable_file_exists(&dir.join(bin)))
    })
}

pub(crate) fn executable_file_exists(path: &std::path::Path) -> bool {
    if path.is_file() {
        return true;
    }
    #[cfg(windows)]
    if path.extension().is_none() {
        let extensions =
            std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
        return extensions.split(';').any(|extension| {
            let extension = extension.trim().trim_start_matches('.');
            !extension.is_empty() && path.with_extension(extension).is_file()
        });
    }
    false
}

pub(crate) async fn execute_container_backend_candidate(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    tool: &ToolDef,
    candidate: ToolBackendCandidate,
) -> Result<ContainerOutput, StepError> {
    let (runtime, mut args) =
        container_runtime_command(&ctx.run.contract.manifest.permissions.containers)
            .ok_or_else(|| StepError::failed(&node.id, "container runtime was not found"))?;
    let image = candidate
        .container_image
        .ok_or_else(|| StepError::failed(&node.id, "container image was not resolved"))?;
    args.extend([
        "--rm".to_string(),
        "--network".to_string(),
        "none".to_string(),
        "--read-only".to_string(),
        "--cap-drop".to_string(),
        "ALL".to_string(),
        "--security-opt".to_string(),
        "no-new-privileges".to_string(),
        "--pids-limit".to_string(),
        "256".to_string(),
        "--tmpfs".to_string(),
        "/tmp:rw,noexec,nosuid,size=64m".to_string(),
    ]);
    let cidfile_name = format!(
        ".qcg-container-{}.cid",
        hex::encode(Sha256::digest(node.id.as_bytes()))
    );
    let cidfile = ctx.run.fs.workspace().join(&cidfile_name);
    args.push("--cidfile".into());
    args.push(cidfile.to_string());
    for mount in &candidate.container_mounts {
        args.push("-v".into());
        args.push(format!(
            "{}:{}:{}",
            ctx.run.fs.workspace(),
            mount.target,
            mount.mode
        ));
    }
    args.push(image);
    args.extend(candidate.argv);
    let argv = std::iter::once(runtime.clone())
        .chain(args)
        .collect::<Vec<_>>();
    let result = ctx
        .spawn_process(
            node,
            &argv,
            tool.timeout_seconds,
            Some(tool.output_limit_bytes),
        )
        .await;
    if result.as_ref().is_err_and(StepError::is_cancelled)
        && let Ok(bytes) = bounded_file_bytes(&cidfile, Some(1024)).await
        && let Ok(container_id) = String::from_utf8(bytes)
    {
        ctx.kill_container(&runtime, container_id.trim()).await;
    }
    let _ = tokio::fs::remove_file(&cidfile).await;
    let output = result?;
    Ok(ContainerOutput {
        runtime,
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

pub(crate) fn container_runtime_command(
    permission: &qcg_contract::ContainerPermission,
) -> Option<(String, Vec<String>)> {
    let runtime = permission.runtime.as_ref()?;
    let (binary, runtime_arg) = match runtime {
        ContainerRuntime::Docker => ("docker", None),
        ContainerRuntime::Podman => ("podman", None),
        ContainerRuntime::DockerRunsc => ("docker", Some("runsc")),
    };
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .any(|dir| dir.join(binary).is_file())
        .then(|| {
            let mut args = vec!["run".to_string()];
            if let Some(runtime) = runtime_arg {
                args.extend(["--runtime".into(), runtime.into()]);
            }
            (binary.to_string(), args)
        })
}
