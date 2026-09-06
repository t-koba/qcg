use qcg_policy::credential_like_name;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::transport::{
    McpAuth, McpLifecycle, McpTransport, OAuthCredentialStore, default_auth, default_lifecycle,
    default_max_response_bytes, default_oauth_store, default_timeout_seconds, default_transport,
};
use super::validate::{
    dangerous_process_env_name, reserved_transport_header, valid_env_name, valid_id, validate_host,
    validate_remote_url,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
    pub id: String,
    #[serde(default = "default_transport")]
    pub transport: McpTransport,
    #[serde(default = "default_lifecycle")]
    pub lifecycle: McpLifecycle,
    #[serde(default)]
    pub url: Option<String>,
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
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for McpServerSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpServerSpec")
            .field("id", &self.id)
            .field("transport", &self.transport)
            .field("lifecycle", &self.lifecycle)
            .field("url", &self.url.as_ref().map(|_| "<configured>"))
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
            .field("timeout_seconds", &self.timeout_seconds)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

impl McpServerSpec {
    pub fn validate(&self) -> Result<(), String> {
        if !valid_id(&self.id) {
            return Err(format!(
                "MCP server id `{}` must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`",
                self.id
            ));
        }
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
            if dangerous_process_env_name(target) {
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
            .find(|name| dangerous_process_env_name(name))
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
        match self.transport {
            McpTransport::StreamableHttp => {
                if !self.command.is_empty() || !self.env.is_empty() || !self.env_from.is_empty() {
                    return Err(format!(
                        "MCP server `{}` streamable_http transport must not declare command or environment fields",
                        self.id
                    ));
                }
                let raw = self.url.as_deref().ok_or_else(|| {
                    format!(
                        "MCP server `{}` streamable_http transport requires url",
                        self.id
                    )
                })?;
                let url = validate_remote_url(&self.id, raw)?;
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
            }
            McpTransport::Stdio => {
                if self.url.is_some() || !self.headers.is_empty() || !self.allowed_hosts.is_empty()
                {
                    return Err(format!(
                        "MCP server `{}` stdio transport must not declare HTTP fields",
                        self.id
                    ));
                }
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
            }
        }
        match self.auth {
            McpAuth::None => {
                if self.credential_env.is_some()
                    || self.auth_header.is_some()
                    || !self.auth_prefix.is_empty()
                    || self.oauth_client_id_env.is_some()
                    || self.oauth_client_secret_env.is_some()
                    || !self.oauth_scopes.is_empty()
                {
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
                if self.credential_env.is_some()
                    || self.auth_header.is_some()
                    || !self.auth_prefix.is_empty()
                {
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

    fn reject_oauth_fields(&self) -> Result<(), String> {
        if self.oauth_client_id_env.is_some()
            || self.oauth_client_secret_env.is_some()
            || !self.oauth_scopes.is_empty()
        {
            return Err(format!(
                "MCP server `{}` non-OAuth auth must not declare OAuth fields",
                self.id
            ));
        }
        Ok(())
    }
}
