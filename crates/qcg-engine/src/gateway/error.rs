use camino::Utf8PathBuf;

use super::fs::CommandPermissionSummary;

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
