//! Canonical destination-host validation shared by every network policy.
//!
//! Hosts are matched as strings, never resolved here; this module only
//! refuses spellings that could smuggle a URL, a port, or a path into an
//! allowlist entry.

use url::Url;

/// Accepts a canonical host name or a bracketed IPv6 literal, the same
/// spelling `Url::host_str` produces for an authority. Ports, paths,
/// credentials, wildcards, and non-canonical spellings are refused.
pub fn validate_host(host: &str) -> Result<(), String> {
    if let Some(inner) = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        // Bracketed IPv6 literal only: the inner text must parse as an IPv6
        // address and the bracketed form must round-trip through `Url`
        // unchanged (canonical lowercase, no port or zone suffix).
        let valid = !inner.is_empty()
            && inner.parse::<std::net::Ipv6Addr>().is_ok()
            && Url::parse(&format!("https://{host}"))
                .ok()
                .and_then(|url| url.host_str().map(str::to_string))
                .as_deref()
                == Some(host);
        if valid {
            return Ok(());
        }
        return Err(format!("`{host}` is not a canonical host name"));
    }
    if host.is_empty()
        || host.contains(['/', ':', '@', '?', '#'])
        || Url::parse(&format!("https://{host}"))
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
            .as_deref()
            != Some(host)
    {
        return Err(format!("`{host}` is not a canonical host name"));
    }
    Ok(())
}
