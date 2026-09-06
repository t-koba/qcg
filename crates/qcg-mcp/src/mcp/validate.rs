use rmcp::transport::auth::AuthError;
use serde::Serialize;
use url::Url;

use super::error::McpError;

pub(crate) fn required_env(name: &str) -> Result<String, McpError> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) | Err(std::env::VarError::NotPresent) => Err(McpError::Configuration(format!(
            "set `{name}` before using the MCP server"
        ))),
        Err(std::env::VarError::NotUnicode(_)) => Err(McpError::Configuration(format!(
            "environment variable `{name}` is not valid UTF-8"
        ))),
    }
}

pub(crate) fn auth_error(_error: AuthError) -> McpError {
    // OAuth and credential-store errors may contain token response bodies or
    // platform-specific secret-store details. Keep the public error stable and
    // deliberately omit the provider-supplied message.
    McpError::Authorization("authorization protocol operation failed".into())
}

pub(crate) fn guarded_transport_error(
    error: impl std::fmt::Display,
    sensitive_values: &[String],
) -> McpError {
    let message = error.to_string();
    if contains_sensitive_value(&message, sensitive_values) {
        McpError::Transport("MCP server reflected credential material in an error".into())
    } else {
        McpError::Transport(message)
    }
}

pub(crate) fn reject_credential_reflection(
    value: &impl Serialize,
    sensitive_values: &[String],
) -> Result<(), McpError> {
    if sensitive_values.is_empty() {
        return Ok(());
    }
    let encoded =
        serde_json::to_string(value).map_err(|error| McpError::Transport(error.to_string()))?;
    if contains_sensitive_value(&encoded, sensitive_values) {
        return Err(McpError::Transport(
            "MCP server reflected credential material in a response".into(),
        ));
    }
    Ok(())
}

fn contains_sensitive_value(text: &str, sensitive_values: &[String]) -> bool {
    sensitive_values
        .iter()
        .filter(|value| !value.is_empty())
        .any(|value| text.contains(value))
}

pub(crate) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

pub(crate) fn valid_env_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

pub(crate) fn validate_oauth_operation_url(
    url: &Url,
) -> Result<(), rmcp::transport::auth::OAuthHttpClientError> {
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("OAuth request URL must not contain credentials or a fragment".into());
    }
    Ok(())
}

pub(crate) fn dangerous_process_env_name(value: &str) -> bool {
    matches!(
        value,
        "PATH"
            | "LD_PRELOAD"
            | "LD_LIBRARY_PATH"
            | "DYLD_INSERT_LIBRARIES"
            | "DYLD_LIBRARY_PATH"
            | "NODE_OPTIONS"
            | "PYTHONPATH"
            | "PYTHONHOME"
            | "RUBYOPT"
            | "PERL5OPT"
    )
}

pub(crate) fn reserved_transport_header(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "accept" | "authorization" | "content-type" | "mcp-session-id" | "last-event-id"
    )
}

pub(crate) fn validate_remote_url(id: &str, raw: &str) -> Result<Url, String> {
    let url =
        Url::parse(raw).map_err(|error| format!("MCP server `{id}` has invalid url: {error}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(format!(
            "MCP server `{id}` url must be HTTP(S) without credentials, query, or fragment"
        ));
    }
    if url.scheme() != "https" && !is_loopback(url.host_str().expect("validated host")) {
        return Err(format!("MCP server `{id}` remote url must use HTTPS"));
    }
    Ok(url)
}

pub(crate) fn is_secure_remote_url(url: &Url) -> bool {
    url.scheme() == "https" || (url.scheme() == "http" && url.host_str().is_some_and(is_loopback))
}

pub(crate) fn validate_redirect_uri(raw: &str) -> Result<Url, McpError> {
    let url = Url::parse(raw).map_err(|error| McpError::Configuration(error.to_string()))?;
    let host = url
        .host_str()
        .ok_or_else(|| McpError::Configuration("OAuth redirect URI must contain a host".into()))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || (url.scheme() != "https" && !(url.scheme() == "http" && is_loopback(host)))
    {
        return Err(McpError::Configuration(
            "OAuth redirect URI must use HTTPS, or HTTP on a loopback host, without credentials or a fragment"
                .into(),
        ));
    }
    Ok(url)
}

pub(crate) fn validate_host(host: &str) -> Result<(), String> {
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

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}
