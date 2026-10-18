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
use crate::{JournalWriter, StepError};
use serde_json::json;

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

/// Records one delegated destination verdict for audit. The event keeps
/// the existing hosts-only payload; URLs, query values and arguments are
/// never journaled.
pub fn record_destination_decision(
    journal: &JournalWriter,
    node: &str,
    server: &str,
    tool: &str,
    hosts: &[String],
    timeout_seconds: u64,
    allow: bool,
) -> Result<(), StepError> {
    journal
        .event(
            "mcp_destination_decision",
            json!({
                "node": node,
                "server": server,
                "tool": tool,
                "hosts": hosts,
                "verdict_source": "decider",
                "allow": allow,
                "timeout_seconds": timeout_seconds,
            }),
        )
        .map_err(|error| StepError::failed(node, error.to_string()))
}

#[cfg(all(test, unix))]
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

#[cfg(test)]
mod journal_tests {
    use super::record_destination_decision;
    use crate::JournalWriter;
    use camino::Utf8PathBuf;
    use serde_json::{Value, json};

    #[test]
    fn destination_decision_writer_preserves_hosts_only_event() {
        let root = std::env::temp_dir().join(format!(
            "mcp-destination-decision-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        let path = Utf8PathBuf::from_path_buf(root.join("journal.jsonl"))
            .expect("temporary journal path should be UTF-8");
        let journal = JournalWriter::create(&path, "decision-test", false, None)
            .expect("journal should be created");
        let hosts = vec!["example.test".to_string()];

        record_destination_decision(
            &journal,
            "fetch-node",
            "exa-public",
            "fetch",
            &hosts,
            7,
            false,
        )
        .expect("destination decision should be recorded");

        drop(journal);
        let contents = std::fs::read_to_string(&path).expect("journal should be readable");
        let event: Value = serde_json::from_str(contents.lines().next().unwrap())
            .expect("journal event should be valid JSON");
        assert_eq!(event["t"], "mcp_destination_decision");
        assert_eq!(event["node"], "fetch-node");
        assert_eq!(event["server"], "exa-public");
        assert_eq!(event["tool"], "fetch");
        assert_eq!(event["hosts"], json!(["example.test"]));
        assert_eq!(event["verdict_source"], "decider");
        assert_eq!(event["allow"], false);
        assert_eq!(event["timeout_seconds"], 7);
        assert!(event.get("arguments").is_none());
        std::fs::remove_dir_all(&root).expect("temporary journal directory should be removed");
    }

    #[test]
    fn destination_decision_writer_maps_journal_errors_to_node_failures() {
        let root = std::env::temp_dir().join(format!(
            "mcp-destination-decision-error-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        let path = Utf8PathBuf::from_path_buf(root.join("journal.jsonl"))
            .expect("temporary journal path should be UTF-8");
        let limits = crate::JournalLimits {
            max_event_bytes: Some(1),
            max_total_bytes: None,
            max_event_count: None,
            max_state_bytes: None,
            scan_window_bytes: None,
        };
        let journal =
            JournalWriter::create_with_limits(&path, "decision-test", false, None, limits)
                .expect("journal should be created with bounded event size");
        let error = record_destination_decision(
            &journal,
            "fetch-node",
            "exa-public",
            "fetch",
            &["example.test".to_string()],
            7,
            false,
        )
        .expect_err("oversized event should fail");
        assert!(matches!(
            error,
            crate::StepError::Failed { node, message }
                if node == "fetch-node" && message.contains("journal event exceeds 1 bytes")
        ));
        drop(journal);
        std::fs::remove_dir_all(&root).expect("temporary journal directory should be removed");
    }
}
