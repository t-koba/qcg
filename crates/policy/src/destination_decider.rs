//! Fail-closed delegation for MCP server-side fetch destinations.
//!
//! Mechanism/policy split: the destination choke point extracts whole-value
//! `http(s)` hosts ([`crate::extract_mcp_destinations`]) and matches them
//! against the static `allowed_destinations` ceiling. When that static match
//! misses and the operator configured a per-server `destination_decider`
//! command, the gate hands the decision to operator policy instead of
//! forcing a choice between refusing the fetch and allowing everything with
//! `"*"`. The static lists stay the built-in default: unset means current
//! behavior exactly.
//!
//! The decider is fallback-only: it can only widen a static miss, never a
//! per-tool narrowing denial, and static hits never consult it. Request
//! facts travel as bounded JSON on stdin (`{v,server,tool,hosts}` — hosts
//! only, never URLs, query values, or arguments, so no secret reaches the
//! decider); the decider answers bounded JSON on stdout (`{v,allow}`).
//! Anything else — unset, spawn failure, timeout, non-zero exit, oversize
//! or invalid output — denies.

/// Protocol version spoken on both stdin and stdout.
pub const DECIDER_PROTOCOL_VERSION: u32 = 1;
/// Default bound when the row omits `destination_decider_timeout_seconds`.
pub const DEFAULT_DECIDER_TIMEOUT_SECONDS: u64 = 5;
/// Mechanistic bounds for `destination_decider_timeout_seconds`.
pub const MIN_DECIDER_TIMEOUT_SECONDS: u64 = 1;
/// Mechanistic bounds for `destination_decider_timeout_seconds`.
pub const MAX_DECIDER_TIMEOUT_SECONDS: u64 = 10;
/// Input/output byte caps for one decider invocation (hosts lists cannot
/// legitimately exceed these; anything larger denies).
pub const DECIDER_INPUT_LIMIT_BYTES: usize = 4_096;
/// Input/output byte caps for one decider invocation.
pub const DECIDER_OUTPUT_LIMIT_BYTES: usize = 4_096;

/// Builds the bounded stdin request for one delegated decision. Hosts are
/// sorted and deduplicated; an empty set never reaches the decider (host-free
/// calls pass without consulting it).
pub fn decider_request_json(server: &str, tool: &str, hosts: &[String]) -> Vec<u8> {
    let mut sorted = hosts.to_vec();
    sorted.sort();
    sorted.dedup();
    serde_json::to_vec(&serde_json::json!({
        "v": DECIDER_PROTOCOL_VERSION,
        "server": server,
        "tool": tool,
        "hosts": sorted,
    }))
    .expect("decider request is always serializable")
}

/// Strict verdict parser: only exit zero plus exact `{v:1, allow:bool}`
/// allows. Unknown versions, missing or non-boolean `allow`, trailing
/// garbage after the JSON document, and any failure all deny.
pub fn decider_verdict_allows(status: i32, stdout: &[u8]) -> bool {
    if status != 0 {
        return false;
    }
    if stdout.len() > DECIDER_OUTPUT_LIMIT_BYTES {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(stdout) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.get("v").and_then(serde_json::Value::as_u64) != Some(DECIDER_PROTOCOL_VERSION as u64)
    {
        return false;
    }
    // No trailing garbage: the whole document must be the verdict object.
    // `serde_json::from_slice` already rejects trailing characters, so a
    // successful parse plus the shape checks below is sufficient.
    if object.len() != 2 {
        return false;
    }
    matches!(object.get("allow"), Some(serde_json::Value::Bool(true)))
}

/// Resolves the effective decider timeout: the row value when set, else the
/// documented default. Validation bounds the row value; this clamps
/// defensively so callers never inherit an unbounded wait.
pub fn effective_decider_timeout_seconds(configured: Option<u64>) -> u64 {
    match configured {
        Some(timeout) => timeout.clamp(MIN_DECIDER_TIMEOUT_SECONDS, MAX_DECIDER_TIMEOUT_SECONDS),
        None => DEFAULT_DECIDER_TIMEOUT_SECONDS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_carries_sorted_hosts_without_urls_or_values() {
        let bytes = decider_request_json(
            "exa-public",
            "fetch",
            &[
                "b.test".to_string(),
                "a.test".to_string(),
                "a.test".to_string(),
            ],
        );
        assert!(bytes.len() <= DECIDER_INPUT_LIMIT_BYTES);
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(value["v"], 1);
        assert_eq!(value["server"], "exa-public");
        assert_eq!(value["tool"], "fetch");
        assert_eq!(value["hosts"], serde_json::json!(["a.test", "b.test"]));
        let text = String::from_utf8(bytes).expect("UTF-8");
        assert!(!text.contains("https://"), "no URLs travel to the decider");
        assert!(!text.contains('?'), "no query values travel to the decider");
    }

    #[test]
    fn only_exact_allow_verdict_passes() {
        assert!(decider_verdict_allows(0, br#"{"v":1,"allow":true}"#));
        for denied in [
            &br#"{"v":1,"allow":false}"#[..],
            br#"{"v":1}"#,
            br#"{"v":2,"allow":true}"#,
            br#"{"v":1,"allow":true,"reason":"extra"}"#,
            br#"{"v":1,"allow":"yes"}"#,
            br#"not json"#,
            br#"{"v":1,"allow":true} trailing"#,
            br#""#,
        ] {
            assert!(
                !decider_verdict_allows(0, denied),
                "must deny: {}",
                String::from_utf8_lossy(denied)
            );
        }
        assert!(!decider_verdict_allows(1, br#"{"v":1,"allow":true}"#));
        assert!(!decider_verdict_allows(-1, br#"{"v":1,"allow":true}"#));
    }

    #[test]
    fn timeout_resolution_clamps_to_bounds() {
        assert_eq!(
            effective_decider_timeout_seconds(None),
            DEFAULT_DECIDER_TIMEOUT_SECONDS
        );
        assert_eq!(effective_decider_timeout_seconds(Some(3)), 3);
        assert_eq!(
            effective_decider_timeout_seconds(Some(0)),
            MIN_DECIDER_TIMEOUT_SECONDS
        );
        assert_eq!(
            effective_decider_timeout_seconds(Some(u64::MAX)),
            MAX_DECIDER_TIMEOUT_SECONDS
        );
    }
}
