//! Bounded destination-decider invocation shared by both MCP gates.
//!
//! The decider is fallback-only operator policy: it runs only after the
//! static `allowed_destinations` ceiling misses, receives hosts-only
//! request facts on stdin, and answers a strict boolean verdict on stdout.
//! See `policy::destination_decider` for the protocol. Every gateway error
//! denies except cancellation, which propagates so node timeouts and parent
//! cancels still stop the wait.

use super::command::CmdGateway;
use super::error::GatewayError;

/// Consults the operator decider for statically-missed hosts. `Ok(true)`
/// allows the fetch; `Ok(false)` denies it (any spawn failure, timeout,
/// non-zero exit, or malformed verdict). `Err` carries only cancellation or
/// gateway-level failures the caller maps explicitly.
pub async fn decide_destination(
    gateway: &CmdGateway,
    server: &str,
    tool: &str,
    hosts: &[String],
    argv: &[String],
    timeout_seconds: u64,
) -> Result<bool, GatewayError> {
    let stdin = policy::decider_request_json(server, tool, hosts);
    if stdin.len() > policy::DECIDER_INPUT_LIMIT_BYTES {
        return Ok(false);
    }
    let output = gateway
        .run_with_limits_and_stdin(
            argv,
            timeout_seconds,
            Some(policy::DECIDER_OUTPUT_LIMIT_BYTES),
            Some(&stdin),
        )
        .await?;
    Ok(policy::decider_verdict_allows(
        output.status,
        &output.stdout_bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use contract::{CommandIsolation, CommandPermission, Permissions};

    fn test_workspace() -> Utf8PathBuf {
        let dir =
            std::env::temp_dir().join(format!("decider-test-{}", uuid::Uuid::now_v7().as_simple()));
        std::fs::create_dir_all(&dir).expect("test workspace should be created");
        Utf8PathBuf::from_path_buf(dir).expect("temp dir should be UTF-8")
    }

    fn gateway_with_sh(workspace: Utf8PathBuf) -> CmdGateway {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "sh".into(),
            args: vec!["-c".into(), "*".into()],
            purpose: "destination decider test".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        CmdGateway::new(permissions, workspace)
    }

    fn hosts() -> Vec<String> {
        vec!["example.test".to_string()]
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn allow_verdict_permits_the_fetch() {
        let allowed = decide_destination(
            &gateway_with_sh(test_workspace()),
            "exa-public",
            "fetch",
            &hosts(),
            &[
                "sh".into(),
                "-c".into(),
                "cat >/dev/null; printf '{\"v\":1,\"allow\":true}'".into(),
            ],
            5,
        )
        .await
        .expect("decider should run");
        assert!(allowed);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deny_verdict_and_failures_refuse() {
        let gateway = gateway_with_sh(test_workspace());
        // Explicit deny.
        let allowed = decide_destination(
            &gateway,
            "exa-public",
            "fetch",
            &hosts(),
            &[
                "sh".into(),
                "-c".into(),
                "cat >/dev/null; printf '{\"v\":1,\"allow\":false}'".into(),
            ],
            5,
        )
        .await
        .expect("decider should run");
        assert!(!allowed);
        // Non-zero exit denies.
        let allowed = decide_destination(
            &gateway,
            "exa-public",
            "fetch",
            &hosts(),
            &["sh".into(), "-c".into(), "cat >/dev/null; exit 1".into()],
            5,
        )
        .await
        .expect("decider should run");
        assert!(!allowed);
        // Garbage output denies.
        let allowed = decide_destination(
            &gateway,
            "exa-public",
            "fetch",
            &hosts(),
            &[
                "sh".into(),
                "-c".into(),
                "cat >/dev/null; echo garbage".into(),
            ],
            5,
        )
        .await
        .expect("decider should run");
        assert!(!allowed);
        // Undeclared binary denies at the gateway (no command permission).
        let denied = decide_destination(
            &gateway,
            "exa-public",
            "fetch",
            &hosts(),
            &["no-such-decider".into()],
            5,
        )
        .await;
        assert!(denied.is_err(), "undeclared decider must fail closed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_denies_without_hanging_the_gate() {
        let denied = decide_destination(
            &gateway_with_sh(test_workspace()),
            "exa-public",
            "fetch",
            &hosts(),
            &["sh".into(), "-c".into(), "cat >/dev/null; sleep 30".into()],
            1,
        )
        .await;
        assert!(
            matches!(denied, Err(GatewayError::CommandTimedOut { .. })),
            "timeout must surface distinctly: {denied:?}"
        );
    }
}
