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
    #[error(transparent)]
    Container(#[from] container::ContainerError),
    #[error("command `{bin}` timed out")]
    CommandTimedOut { bin: String },
    #[error("command `{bin}` output exceeded limit")]
    CommandOutputTooLarge { bin: String },
    #[error("command `{bin}` input exceeded limit")]
    CommandInputTooLarge { bin: String },
    #[error("network access to host `{host}` is not allowed by permissions.network")]
    NetworkDenied { host: String },
    #[error(
        "network access to host `{host}` is denied: resolved IP `{ip}` is not globally reachable (permissions.network_deny_private_ips)"
    )]
    NetworkIpDenied { host: String, ip: String },
    #[error("unsupported URL `{url}`")]
    UnsupportedUrl { url: String },
    #[error("HTTP response body exceeded limit for `{url}`")]
    HttpBodyTooLarge { url: String },
    #[error("HTTP request body exceeded limit for `{url}`")]
    HttpRequestBodyTooLarge { url: String },
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// A failure raised after this operation already dispatched a request
    /// that may have applied effects. The underlying error's shape no
    /// longer proves non-application, whatever its kind (D02).
    #[error("side-effect-bearing request was already sent when the operation failed: {0}")]
    AfterSend(Box<GatewayError>),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("execution was canceled")]
    Canceled,
    /// Hash-anchored patch mechanism failure. Policy layers match on the
    /// inner variant instead of parsing message strings.
    #[error(transparent)]
    AnchoredPatch(#[from] files::AnchoredPatchError),
    #[error("file `{path}` is not valid UTF-8 for anchored patch")]
    PatchNotUtf8 { path: String },
}

/// Formats an anchored-patch failure with a caller-provided prefix.
///
/// Stale anchors render their remaps so the caller can show what moved;
/// every other rejection names its cause. Pure over the error value so
/// step flows share one formatter while keeping caller-specific prefixes.
pub fn anchored_patch_error_message(error: &files::AnchoredPatchError, prefix: &str) -> String {
    use files::AnchoredPatchError as Error;
    match error {
        Error::AnchorStale {
            anchor,
            remaps,
            current_base,
        } => {
            let hints: Vec<String> = remaps
                .iter()
                .map(|remap| format!("{} -> {}", remap.stale_anchor, remap.current_anchor))
                .collect();
            format!(
                "{prefix} anchor `{anchor}` is stale (base `{current_base}`); remaps: {}",
                hints.join(", ")
            )
        }
        other => format!("{prefix} rejected: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::anchored_patch_error_message;

    fn stale_error() -> files::AnchoredPatchError {
        files::AnchoredPatchError::AnchorStale {
            anchor: "2:deadbeef".into(),
            remaps: vec![
                files::AnchorRemap {
                    stale_anchor: "2:deadbeef".into(),
                    current_anchor: "2:015cadfe".into(),
                },
                files::AnchorRemap {
                    stale_anchor: "3:aaaa".into(),
                    current_anchor: "4:bbbb".into(),
                },
            ],
            current_base: "abc".into(),
        }
    }

    #[test]
    fn stale_remap_pins_patch_prefix() {
        assert_eq!(
            anchored_patch_error_message(&stale_error(), "patch"),
            "patch anchor `2:deadbeef` is stale (base `abc`); remaps: 2:deadbeef -> 2:015cadfe, 3:aaaa -> 4:bbbb"
        );
    }

    #[test]
    fn stale_remap_pins_repair_prefix() {
        assert_eq!(
            anchored_patch_error_message(&stale_error(), "repair patch"),
            "repair patch anchor `2:deadbeef` is stale (base `abc`); remaps: 2:deadbeef -> 2:015cadfe, 3:aaaa -> 4:bbbb"
        );
    }

    #[test]
    fn generic_error_pins_both_prefixes() {
        let error = files::AnchoredPatchError::EmptyEdits;
        assert_eq!(
            anchored_patch_error_message(&error, "patch"),
            "patch rejected: patch has no edits"
        );
        assert_eq!(
            anchored_patch_error_message(&error, "repair patch"),
            "repair patch rejected: patch has no edits"
        );
    }
}
