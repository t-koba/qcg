use policy::{MAX_DECIDER_TIMEOUT_SECONDS, MIN_DECIDER_TIMEOUT_SECONDS, credential_like_name};
use serde::Deserialize;
use std::collections::BTreeMap;
use url::Url;

pub(crate) const TOOLS_LIST_PAGE_LIMIT_MIN: usize = 1;

pub(crate) const TOOLS_LIST_PAGE_LIMIT_MAX: usize = 1_000;

const NON_NETWORK_TOOL_NAME_MAX_BYTES: usize = 128;

const OAUTH_STATE_TTL_SECONDS_MIN: u64 = 60;

const OAUTH_STATE_TTL_SECONDS_MAX: u64 = 3_600;

pub(crate) const TASK_POLL_INTERVAL_MS_MIN: u64 = 50;

pub(crate) const TASK_POLL_INTERVAL_MS_MAX: u64 = 5_000;

use super::transport::{
    McpAuth, McpLifecycle, McpTransport, OAuthCredentialStore, default_auth, default_lifecycle,
    default_max_response_bytes, default_oauth_state_ttl_seconds, default_oauth_store,
    default_task_poll_interval_ms, default_timeout_seconds, default_tools_list_page_limit,
    default_transport,
};
use super::validate::{
    forbidden_process_env_name, reserved_transport_header, valid_env_name, valid_id,
    validate_destination, validate_host, validate_remote_url_with_policy,
};

/// Where a server definition was authored. A generator contract, skill, or
/// tool-described server can only name an already configured server; it never
/// supplies a definition. Definitions rebuilt from such data keep `Untrusted`
/// and remain under the process-control environment refusal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum McpSpecOrigin {
    #[default]
    Untrusted,
    /// A row parsed from the operator-controlled providers registry.
    OperatorRegistry,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
    pub id: String,
    /// Serde deliberately cannot set this: only the registry parse choke
    /// points call [`McpServerSpec::mark_operator_registry`]. It is included
    /// in `Debug` so a trusted row is auditable in diagnostics.
    #[serde(skip)]
    pub origin: McpSpecOrigin,
    #[serde(default = "default_transport")]
    pub transport: McpTransport,
    #[serde(default = "default_lifecycle")]
    pub lifecycle: McpLifecycle,
    #[serde(default)]
    pub url: Option<String>,
    /// Allows credential-free HTTP to a literal private IP for a regular remote row.
    #[serde(default)]
    pub allow_plaintext_private_http: bool,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub env_from: BTreeMap<String, String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_auth")]
    pub auth: McpAuth,
    #[serde(default)]
    pub credential_env: Option<String>,
    #[serde(default)]
    pub auth_header: Option<String>,
    #[serde(default)]
    pub auth_prefix: String,
    #[serde(default)]
    pub oauth_scopes: Vec<String>,
    #[serde(default)]
    pub oauth_client_id_env: Option<String>,
    #[serde(default)]
    pub oauth_client_secret_env: Option<String>,
    #[serde(default = "default_oauth_store")]
    pub oauth_store: OAuthCredentialStore,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// Operator ceiling for server-side fetch destinations extracted from
    /// tool arguments. Empty (default) denies every extracted host; calls
    /// with no extracted host pass. Applies to both transports: a stdio
    /// server fetches remotely too.
    #[serde(default)]
    pub allowed_destinations: Vec<String>,
    /// Remote tool names the operator has verified cannot perform server-side
    /// fetches: these tools never fetch URLs or hosts from their arguments.
    /// Empty (default) keeps every tool under the fail-closed extraction,
    /// ceiling, narrowing, decider and ambiguity rules. Only declare a tool
    /// after verifying its remote implementation never fetches; qcg cannot
    /// re-check what the server does with an argument.
    #[serde(default)]
    pub non_network_tools: Vec<String>,
    /// Optional fail-closed destination decider: an operator-configured
    /// command consulted only when the static `allowed_destinations` ceiling
    /// misses. Empty (default) keeps current behavior exactly. The command
    /// receives `{v,server,tool,hosts}` on stdin and must answer
    /// `{v,allow}` on stdout; anything else denies. The contract must still
    /// allow the binary through `permissions.commands`.
    #[serde(default)]
    pub destination_decider: Vec<String>,
    /// Bound for one decider invocation. `None` (default) means 5 seconds;
    /// when set must sit within 1..=10.
    #[serde(default)]
    pub destination_decider_timeout_seconds: Option<u64>,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
    #[serde(default = "default_tools_list_page_limit")]
    pub tools_list_page_limit: usize,
    #[serde(default = "default_oauth_state_ttl_seconds")]
    pub oauth_state_ttl_seconds: u64,
    #[serde(default = "default_task_poll_interval_ms")]
    pub task_poll_interval_ms: u64,
}

impl std::fmt::Debug for McpServerSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpServerSpec")
            .field("id", &self.id)
            .field("origin", &self.origin)
            .field("transport", &self.transport)
            .field("lifecycle", &self.lifecycle)
            .field("url", &self.url.as_ref().map(|_| "<configured>"))
            .field(
                "allow_plaintext_private_http",
                &self.allow_plaintext_private_http,
            )
            .field("command_bin", &self.command.first())
            .field("command_arg_count", &self.command.len().saturating_sub(1))
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .field("env_from", &self.env_from)
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("auth", &self.auth)
            .field("credential_env", &self.credential_env)
            .field("auth_header", &self.auth_header)
            .field(
                "auth_prefix",
                &(!self.auth_prefix.is_empty()).then_some("<redacted>"),
            )
            .field("oauth_scopes", &self.oauth_scopes)
            .field("oauth_client_id_env", &self.oauth_client_id_env)
            .field("oauth_client_secret_env", &self.oauth_client_secret_env)
            .field("oauth_store", &self.oauth_store)
            .field("allowed_hosts", &self.allowed_hosts)
            .field("allowed_destinations", &self.allowed_destinations)
            .field("non_network_tools", &self.non_network_tools)
            .field("destination_decider_bin", &self.destination_decider.first())
            .field(
                "destination_decider_arg_count",
                &self.destination_decider.len().saturating_sub(1),
            )
            .field(
                "destination_decider_timeout_seconds",
                &self.destination_decider_timeout_seconds,
            )
            .field("timeout_seconds", &self.timeout_seconds)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("tools_list_page_limit", &self.tools_list_page_limit)
            .field("oauth_state_ttl_seconds", &self.oauth_state_ttl_seconds)
            .field("task_poll_interval_ms", &self.task_poll_interval_ms)
            .finish()
    }
}

impl McpServerSpec {
    /// Marks a row as operator registry configuration. Called only where the
    /// providers registry (or shipped public table) is parsed, never from
    /// serialized or model-visible data.
    pub fn mark_operator_registry(&mut self) {
        self.origin = McpSpecOrigin::OperatorRegistry;
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_with_url().map(|_| ())
    }

    pub(crate) fn validate_with_url(&self) -> Result<Option<Url>, String> {
        if !valid_id(&self.id) {
            return Err(format!(
                "MCP server id `{}` must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`",
                self.id
            ));
        }
        // Reject credential configuration before general field checks can report its values.
        let opted_in_url = if self.allow_plaintext_private_http {
            if self.transport != McpTransport::StreamableHttp {
                return Err(
                    "allow_plaintext_private_http requires streamable_http transport".into(),
                );
            }
            let url = self.validate_endpoint_url()?;
            if url.scheme() == "http"
                && (self.auth != McpAuth::None
                    || self.has_static_auth_fields()
                    || !self.headers.is_empty()
                    || self.has_oauth_fields()
                    || self.oauth_store != default_oauth_store()
                    || self.oauth_state_ttl_seconds != default_oauth_state_ttl_seconds())
            {
                return Err("allow_plaintext_private_http requires HTTP endpoints without authentication, static headers, or OAuth settings".into());
            }
            Some(url)
        } else {
            None
        };
        if self.timeout_seconds == 0 {
            return Err(format!(
                "MCP server `{}` timeout_seconds must be greater than zero",
                self.id
            ));
        }
        if self.max_response_bytes == 0 {
            return Err(format!(
                "MCP server `{}` max_response_bytes must be greater than zero",
                self.id
            ));
        }
        if !(TOOLS_LIST_PAGE_LIMIT_MIN..=TOOLS_LIST_PAGE_LIMIT_MAX)
            .contains(&self.tools_list_page_limit)
        {
            return Err(format!(
                "MCP server `{}` tools_list_page_limit must be in {TOOLS_LIST_PAGE_LIMIT_MIN}..={TOOLS_LIST_PAGE_LIMIT_MAX} (got {})",
                self.id, self.tools_list_page_limit
            ));
        }
        if !(OAUTH_STATE_TTL_SECONDS_MIN..=OAUTH_STATE_TTL_SECONDS_MAX)
            .contains(&self.oauth_state_ttl_seconds)
        {
            return Err(format!(
                "MCP server `{}` oauth_state_ttl_seconds must be in {OAUTH_STATE_TTL_SECONDS_MIN}..={OAUTH_STATE_TTL_SECONDS_MAX} (got {})",
                self.id, self.oauth_state_ttl_seconds
            ));
        }
        if !(TASK_POLL_INTERVAL_MS_MIN..=TASK_POLL_INTERVAL_MS_MAX)
            .contains(&self.task_poll_interval_ms)
        {
            return Err(format!(
                "MCP server `{}` task_poll_interval_ms must be in {TASK_POLL_INTERVAL_MS_MIN}..={TASK_POLL_INTERVAL_MS_MAX} (got {})",
                self.id, self.task_poll_interval_ms
            ));
        }
        for (name, value) in &self.headers {
            let header = http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| format!("MCP server `{}` has invalid header `{name}`", self.id))?;
            http::HeaderValue::from_str(value)
                .map_err(|_| format!("MCP server `{}` has invalid header `{name}`", self.id))?;
            if reserved_transport_header(header.as_str()) {
                return Err(format!(
                    "MCP server `{}` static headers must not override MCP transport header `{header}`",
                    self.id
                ));
            }
            if credential_like_name(header.as_str()) {
                return Err(format!(
                    "MCP server `{}` static headers must not contain credentials",
                    self.id
                ));
            }
        }
        for (target, source) in &self.env_from {
            if !valid_env_name(target) || !valid_env_name(source) {
                return Err(format!(
                    "MCP server `{}` env_from must map valid environment variable names",
                    self.id
                ));
            }
            if forbidden_process_env_name(target, self.origin) {
                return Err(format!(
                    "MCP server `{}` env_from must not override process control variable `{target}`",
                    self.id
                ));
            }
        }
        if self.env.keys().any(|name| !valid_env_name(name)) {
            return Err(format!(
                "MCP server `{}` env contains an invalid environment variable name",
                self.id
            ));
        }
        if let Some(name) = self
            .env
            .keys()
            .find(|name| forbidden_process_env_name(name, self.origin))
        {
            return Err(format!(
                "MCP server `{}` env must not override process control variable `{name}`",
                self.id
            ));
        }
        if let Some(name) = self.env.keys().find(|name| credential_like_name(name)) {
            return Err(format!(
                "MCP server `{}` must load sensitive environment variable `{name}` through env_from",
                self.id
            ));
        }
        let url = match self.transport {
            McpTransport::StreamableHttp => {
                if !self.command.is_empty() || !self.env.is_empty() || !self.env_from.is_empty() {
                    return Err(format!(
                        "MCP server `{}` streamable_http transport must not declare command or environment fields",
                        self.id
                    ));
                }
                let url = match opted_in_url {
                    Some(url) => url,
                    None => self.validate_endpoint_url()?,
                };
                let endpoint_host = url.host_str().expect("validated URL has host");
                if !self.allowed_hosts.iter().any(|host| host == endpoint_host) {
                    return Err(format!(
                        "MCP server `{}` allowed_hosts must include endpoint host `{endpoint_host}`",
                        self.id
                    ));
                }
                for host in &self.allowed_hosts {
                    validate_host(host).map_err(|error| {
                        format!("MCP server `{}` has invalid allowed host: {error}", self.id)
                    })?;
                }
                for destination in &self.allowed_destinations {
                    validate_destination(destination).map_err(|error| {
                        format!(
                            "MCP server `{}` has invalid allowed destination: {error}",
                            self.id
                        )
                    })?;
                }
                self.validate_non_network_tools()?;
                self.validate_destination_decider()?;
                Some(url)
            }
            McpTransport::Stdio => {
                if self.url.is_some() || !self.headers.is_empty() || !self.allowed_hosts.is_empty()
                {
                    return Err(format!(
                        "MCP server `{}` stdio transport must not declare HTTP fields",
                        self.id
                    ));
                }
                for destination in &self.allowed_destinations {
                    validate_destination(destination).map_err(|error| {
                        format!(
                            "MCP server `{}` has invalid allowed destination: {error}",
                            self.id
                        )
                    })?;
                }
                self.validate_non_network_tools()?;
                self.validate_destination_decider()?;
                if self
                    .command
                    .first()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return Err(format!(
                        "MCP server `{}` stdio transport requires a non-empty command",
                        self.id
                    ));
                }
                if self.auth != McpAuth::None {
                    return Err(format!(
                        "MCP server `{}` stdio transport must use auth = \"none\"; pass credentials through env_from",
                        self.id
                    ));
                }
                None
            }
        };
        match self.auth {
            McpAuth::None => {
                if self.has_static_auth_fields() || self.has_oauth_fields() {
                    return Err(format!(
                        "MCP server `{}` auth = \"none\" must not declare authentication fields",
                        self.id
                    ));
                }
            }
            McpAuth::Bearer => {
                self.validate_credential_env()?;
                if self.auth_header.is_some() || !self.auth_prefix.is_empty() {
                    return Err(format!(
                        "MCP server `{}` bearer auth uses the Authorization header implicitly",
                        self.id
                    ));
                }
                self.reject_oauth_fields()?;
            }
            McpAuth::Header => {
                self.validate_credential_env()?;
                let header = self.auth_header.as_deref().ok_or_else(|| {
                    format!("MCP server `{}` header auth requires auth_header", self.id)
                })?;
                let header = http::HeaderName::from_bytes(header.as_bytes())
                    .map_err(|_| format!("MCP server `{}` has invalid auth_header", self.id))?;
                if reserved_transport_header(header.as_str()) {
                    return Err(format!(
                        "MCP server `{}` auth_header must not override an MCP transport header",
                        self.id
                    ));
                }
                if self.auth_prefix.contains(['\r', '\n']) {
                    return Err(format!(
                        "MCP server `{}` auth_prefix must not contain line breaks",
                        self.id
                    ));
                }
                self.reject_oauth_fields()?;
            }
            McpAuth::Oauth => {
                if self.has_static_auth_fields() {
                    return Err(format!(
                        "MCP server `{}` OAuth must not declare static credential fields",
                        self.id
                    ));
                }
                if let Some(name) = self.oauth_client_id_env.as_deref()
                    && !valid_env_name(name)
                {
                    return Err(format!(
                        "MCP server `{}` has invalid oauth_client_id_env",
                        self.id
                    ));
                }
                if let Some(name) = self.oauth_client_secret_env.as_deref()
                    && !valid_env_name(name)
                {
                    return Err(format!(
                        "MCP server `{}` has invalid oauth_client_secret_env",
                        self.id
                    ));
                }
                if self.oauth_client_secret_env.is_some() && self.oauth_client_id_env.is_none() {
                    return Err(format!(
                        "MCP server `{}` oauth_client_secret_env requires oauth_client_id_env",
                        self.id
                    ));
                }
                if self
                    .oauth_scopes
                    .iter()
                    .any(|scope| scope.trim().is_empty())
                {
                    return Err(format!(
                        "MCP server `{}` oauth_scopes must not contain empty values",
                        self.id
                    ));
                }
            }
        }
        Ok(url)
    }

    fn validate_endpoint_url(&self) -> Result<Url, String> {
        let raw = self.url.as_deref().ok_or_else(|| {
            format!(
                "MCP server `{}` streamable_http transport requires url",
                self.id
            )
        })?;
        validate_remote_url_with_policy(&self.id, raw, self.allow_plaintext_private_http)
    }

    fn validate_non_network_tools(&self) -> Result<(), String> {
        if self.non_network_tools.len() > TOOLS_LIST_PAGE_LIMIT_MAX {
            return Err(format!(
                "MCP server `{}` non_network_tools must have at most {TOOLS_LIST_PAGE_LIMIT_MAX} entries (got {})",
                self.id,
                self.non_network_tools.len()
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for tool in &self.non_network_tools {
            if tool.trim().is_empty() {
                return Err(format!(
                    "MCP server `{}` non_network_tools must not contain empty or whitespace-only tool names",
                    self.id
                ));
            }
            if tool.chars().any(char::is_control) {
                return Err(format!(
                    "MCP server `{}` non_network_tools must not contain control characters",
                    self.id
                ));
            }
            if tool.len() > NON_NETWORK_TOOL_NAME_MAX_BYTES {
                return Err(format!(
                    "MCP server `{}` non_network_tools tool name exceeds {NON_NETWORK_TOOL_NAME_MAX_BYTES} bytes",
                    self.id
                ));
            }
            if !seen.insert(tool) {
                return Err(format!(
                    "MCP server `{}` non_network_tools contains duplicate tool `{tool}`",
                    self.id
                ));
            }
        }
        Ok(())
    }

    fn validate_destination_decider(&self) -> Result<(), String> {
        if self.destination_decider.is_empty() {
            if self.destination_decider_timeout_seconds.is_some() {
                return Err(format!(
                    "MCP server `{}` destination_decider_timeout_seconds requires destination_decider",
                    self.id
                ));
            }
            return Ok(());
        }
        if self
            .destination_decider
            .first()
            .is_none_or(|bin| bin.trim().is_empty())
        {
            return Err(format!(
                "MCP server `{}` destination_decider requires a non-empty command",
                self.id
            ));
        }
        if let Some(timeout) = self.destination_decider_timeout_seconds
            && !(MIN_DECIDER_TIMEOUT_SECONDS..=MAX_DECIDER_TIMEOUT_SECONDS).contains(&timeout)
        {
            return Err(format!(
                "MCP server `{}` destination_decider_timeout_seconds must be in {MIN_DECIDER_TIMEOUT_SECONDS}..={MAX_DECIDER_TIMEOUT_SECONDS} (got {timeout})",
                self.id
            ));
        }
        Ok(())
    }

    fn validate_credential_env(&self) -> Result<(), String> {
        let name = self.credential_env.as_deref().ok_or_else(|| {
            format!(
                "MCP server `{}` authentication requires credential_env",
                self.id
            )
        })?;
        if !valid_env_name(name) {
            return Err(format!(
                "MCP server `{}` has invalid credential_env",
                self.id
            ));
        }
        Ok(())
    }

    fn has_static_auth_fields(&self) -> bool {
        self.credential_env.is_some() || self.auth_header.is_some() || !self.auth_prefix.is_empty()
    }

    fn has_oauth_fields(&self) -> bool {
        self.oauth_client_id_env.is_some()
            || self.oauth_client_secret_env.is_some()
            || !self.oauth_scopes.is_empty()
    }

    fn reject_oauth_fields(&self) -> Result<(), String> {
        if self.has_oauth_fields() {
            return Err(format!(
                "MCP server `{}` non-OAuth auth must not declare OAuth fields",
                self.id
            ));
        }
        Ok(())
    }
}
