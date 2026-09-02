use reqwest::Url;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMethod {
    Get,
    Post,
}

fn default_search_method() -> SearchMethod {
    SearchMethod::Get
}

fn default_query_param() -> String {
    "q".into()
}

fn default_title_pointer() -> String {
    "/title".into()
}

fn default_url_pointer() -> String {
    "/url".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchProviderSpec {
    pub id: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub endpoint_env: Option<String>,
    #[serde(default = "default_search_method")]
    pub method: SearchMethod,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub body: BTreeMap<String, Value>,
    #[serde(default = "default_query_param")]
    pub query_param: String,
    #[serde(default)]
    pub query_is_array: bool,
    #[serde(default)]
    pub limit_param: Option<String>,
    pub results_pointer: String,
    #[serde(default = "default_title_pointer")]
    pub title_pointer: String,
    #[serde(default = "default_url_pointer")]
    pub url_pointer: String,
    #[serde(default)]
    pub snippet_pointer: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub auth_header: Option<String>,
    #[serde(default)]
    pub auth_query_param: Option<String>,
    #[serde(default)]
    pub auth_prefix: String,
}

impl SearchProviderSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !is_provider_id(&self.id) {
            return Err(format!(
                "search provider id `{}` must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`",
                self.id
            ));
        }
        if self.endpoint.is_none() && self.endpoint_env.is_none() {
            return Err(format!(
                "search provider `{}` must declare `endpoint` or `endpoint_env`",
                self.id
            ));
        }
        if let Some(name) = self.endpoint_env.as_deref()
            && !is_env_name(name)
        {
            return Err(format!(
                "search provider `{}` has an invalid `endpoint_env` environment variable name `{name}`",
                self.id
            ));
        }
        if let Some(name) = self.api_key_env.as_deref()
            && !is_env_name(name)
        {
            return Err(format!(
                "search provider `{}` has an invalid `api_key_env` environment variable name `{name}`",
                self.id
            ));
        }
        if let Some(endpoint) = self.endpoint.as_deref() {
            validate_endpoint(&self.id, endpoint, self.api_key_env.is_some())?;
        }
        validate_parameter_name(&self.id, "query_param", &self.query_param)?;
        if let Some(value) = self.limit_param.as_deref() {
            validate_parameter_name(&self.id, "limit_param", value)?;
        }
        if let Some(value) = self.auth_query_param.as_deref() {
            validate_parameter_name(&self.id, "auth_query_param", value)?;
            if self.query.contains_key(value) {
                return Err(format!(
                    "search provider `{}` static query must not override auth_query_param `{value}`",
                    self.id
                ));
            }
        }
        for name in self.query.keys() {
            validate_parameter_name(&self.id, "static query parameter", name)?;
        }
        if self.body.keys().any(String::is_empty) {
            return Err(format!(
                "search provider `{}` body field names must not be empty",
                self.id
            ));
        }
        if self.method == SearchMethod::Get && (!self.body.is_empty() || self.query_is_array) {
            return Err(format!(
                "search provider `{}` may use `body` and `query_is_array` only with method = \"post\"",
                self.id
            ));
        }
        if self.method == SearchMethod::Get {
            if self.query.contains_key(&self.query_param) {
                return Err(format!(
                    "search provider `{}` static query must not override query_param `{}`",
                    self.id, self.query_param
                ));
            }
            if self
                .limit_param
                .as_ref()
                .is_some_and(|name| self.query.contains_key(name))
            {
                return Err(format!(
                    "search provider `{}` static query must not override limit_param",
                    self.id
                ));
            }
        } else {
            if self.body.contains_key(&self.query_param) {
                return Err(format!(
                    "search provider `{}` static body must not override query_param `{}`",
                    self.id, self.query_param
                ));
            }
            if self
                .limit_param
                .as_ref()
                .is_some_and(|name| self.body.contains_key(name))
            {
                return Err(format!(
                    "search provider `{}` static body must not override limit_param",
                    self.id
                ));
            }
        }
        if self
            .limit_param
            .as_ref()
            .is_some_and(|name| name == &self.query_param)
        {
            return Err(format!(
                "search provider `{}` limit_param must differ from query_param",
                self.id
            ));
        }
        if self.auth_query_param.as_ref().is_some_and(|name| {
            name == &self.query_param || self.limit_param.as_ref() == Some(name)
        }) {
            return Err(format!(
                "search provider `{}` auth_query_param must differ from query_param and limit_param",
                self.id
            ));
        }
        for (header, value) in &self.headers {
            if !valid_http_header_name(header) || value.contains(['\r', '\n']) {
                return Err(format!(
                    "search provider `{}` has an invalid static HTTP header `{header}`",
                    self.id
                ));
            }
            if self
                .auth_header
                .as_deref()
                .is_some_and(|auth| auth.eq_ignore_ascii_case(header))
            {
                return Err(format!(
                    "search provider `{}` static headers must not override auth_header `{header}`",
                    self.id
                ));
            }
        }
        match (
            self.api_key_env.as_deref(),
            self.auth_header.as_deref(),
            self.auth_query_param.as_deref(),
        ) {
            (None, None, None) if self.auth_prefix.is_empty() => {}
            (Some(_), Some(header), None) => {
                if !valid_http_header_name(header) {
                    return Err(format!(
                        "search provider `{}` has an invalid auth_header `{header}`",
                        self.id
                    ));
                }
                if self.auth_prefix.contains(['\r', '\n']) {
                    return Err(format!(
                        "search provider `{}` auth_prefix must not contain a line break",
                        self.id
                    ));
                }
            }
            (Some(_), None, Some(_)) if self.auth_prefix.is_empty() => {}
            _ => {
                return Err(format!(
                    "search provider `{}` must pair api_key_env with exactly one of auth_header or auth_query_param",
                    self.id
                ));
            }
        }
        for (field, value) in [
            ("results_pointer", self.results_pointer.as_str()),
            ("title_pointer", self.title_pointer.as_str()),
            ("url_pointer", self.url_pointer.as_str()),
        ] {
            validate_json_pointer(&self.id, field, value)?;
        }
        if let Some(value) = self.snippet_pointer.as_deref() {
            validate_json_pointer(&self.id, "snippet_pointer", value)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SearchProfile {
    pub id: String,
    pub endpoint: Option<Url>,
    pub method: SearchMethod,
    pub headers: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub body: BTreeMap<String, Value>,
    pub query_param: String,
    pub query_is_array: bool,
    pub limit_param: Option<String>,
    pub results_pointer: String,
    pub title_pointer: String,
    pub url_pointer: String,
    pub snippet_pointer: Option<String>,
    credential_env: Option<String>,
    pub auth_header: Option<String>,
    pub auth_query_param: Option<String>,
    pub auth_prefix: String,
    config_errors: Vec<String>,
}

impl std::fmt::Debug for SearchProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchProfile")
            .field("id", &self.id)
            .field("endpoint", &self.endpoint)
            .field("method", &self.method)
            .field("credential_env", &self.credential_env)
            .field("config_errors", &self.config_errors)
            .finish_non_exhaustive()
    }
}

impl SearchProfile {
    fn from_spec(spec: SearchProviderSpec) -> Self {
        let mut config_errors = Vec::new();
        let raw_endpoint = match spec.endpoint_env.as_deref() {
            Some(name) => match std::env::var(name) {
                Ok(value) => Some(value),
                Err(std::env::VarError::NotPresent) => spec.endpoint.clone(),
                Err(std::env::VarError::NotUnicode(_)) => {
                    config_errors.push(format!("environment variable `{name}` is not valid UTF-8"));
                    None
                }
            },
            None => spec.endpoint.clone(),
        };
        let endpoint = raw_endpoint.and_then(|raw| {
            match validate_endpoint(&spec.id, &raw, spec.api_key_env.is_some()) {
                Ok(url) => Some(url),
                Err(error) => {
                    config_errors.push(error);
                    None
                }
            }
        });
        if endpoint.is_none() && config_errors.is_empty() {
            let source = spec.endpoint_env.as_deref().unwrap_or("endpoint");
            config_errors.push(format!("set `{source}` before running the generator"));
        }
        Self {
            id: spec.id,
            endpoint,
            method: spec.method,
            headers: spec.headers,
            query: spec.query,
            body: spec.body,
            query_param: spec.query_param,
            query_is_array: spec.query_is_array,
            limit_param: spec.limit_param,
            results_pointer: spec.results_pointer,
            title_pointer: spec.title_pointer,
            url_pointer: spec.url_pointer,
            snippet_pointer: spec.snippet_pointer,
            credential_env: spec.api_key_env,
            auth_header: spec.auth_header,
            auth_query_param: spec.auth_query_param,
            auth_prefix: spec.auth_prefix,
            config_errors,
        }
    }

    pub fn host(&self) -> Option<&str> {
        self.endpoint.as_ref().and_then(Url::host_str)
    }

    pub fn credential_env_name(&self) -> Option<&str> {
        self.credential_env.as_deref()
    }

    pub fn credential(&self) -> Result<Option<String>, String> {
        let Some(name) = self.credential_env.as_deref() else {
            return Ok(None);
        };
        match std::env::var(name) {
            Ok(value) if !value.is_empty() => Ok(Some(value)),
            Ok(_) | Err(_) => Err(format!("set `{name}` before running the generator")),
        }
    }

    pub fn configuration_error(&self) -> Option<String> {
        let mut errors = self.config_errors.clone();
        if let Some(name) = self.credential_env.as_deref()
            && !matches!(std::env::var(name), Ok(value) if !value.is_empty())
        {
            errors.push(format!("set `{name}` before running the generator"));
        }
        (!errors.is_empty()).then(|| errors.join("; "))
    }
}

#[derive(Debug, Clone)]
pub struct SearchRuntime {
    profiles: BTreeMap<String, SearchProfile>,
    default_provider: Option<String>,
    pub registry_present: bool,
}

impl SearchRuntime {
    pub fn unavailable() -> Self {
        Self {
            profiles: BTreeMap::new(),
            default_provider: None,
            registry_present: false,
        }
    }

    pub(crate) fn from_specs(
        default_provider: Option<String>,
        specs: Vec<SearchProviderSpec>,
    ) -> Self {
        let profiles = specs
            .into_iter()
            .map(SearchProfile::from_spec)
            .map(|profile| (profile.id.clone(), profile))
            .collect();
        Self {
            profiles,
            default_provider,
            registry_present: true,
        }
    }

    pub fn resolve(&self, requested: Option<&str>) -> Result<&SearchProfile, String> {
        let id = requested
            .filter(|value| !value.trim().is_empty())
            .or(self.default_provider.as_deref())
            .ok_or_else(|| {
                "web.search provider is required because no default search provider is configured"
                    .to_string()
            })?;
        let profile = self.profiles.get(id).ok_or_else(|| {
            let hint = if self.registry_present {
                "enable its row in providers.toml"
            } else {
                "no providers registry was found; pass --providers <PATH>, set QCG_PROVIDERS, or place providers.toml next to the qcg binary"
            };
            format!("search provider `{id}` is not registered; {hint}")
        })?;
        if let Some(error) = profile.configuration_error() {
            return Err(error);
        }
        Ok(profile)
    }

    pub fn default_provider(&self) -> Option<&str> {
        self.default_provider.as_deref()
    }

    pub fn provider_ids(&self) -> Vec<&str> {
        self.profiles.keys().map(String::as_str).collect()
    }

    pub fn credential_env_names(&self) -> Vec<String> {
        self.profiles
            .values()
            .filter_map(|profile| profile.credential_env.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

fn validate_endpoint(id: &str, value: &str, credentialed: bool) -> Result<Url, String> {
    let url = Url::parse(value)
        .map_err(|error| format!("search provider `{id}` has an invalid endpoint: {error}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(format!(
            "search provider `{id}` endpoint must be an HTTP(S) URL without credentials, query, or fragment"
        ));
    }
    if credentialed && url.scheme() != "https" && !is_loopback_host(&url) {
        return Err(format!(
            "search provider `{id}` requires HTTPS for a credentialed remote endpoint"
        ));
    }
    Ok(url)
}

fn validate_parameter_name(provider: &str, field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || matches!(byte, b'&' | b'=' | b'#'))
    {
        return Err(format!(
            "search provider `{provider}` {field} must be a non-empty HTTP parameter name"
        ));
    }
    Ok(())
}

fn validate_json_pointer(provider: &str, field: &str, value: &str) -> Result<(), String> {
    if !value.starts_with('/') || value.split('/').skip(1).any(invalid_json_pointer_escape) {
        return Err(format!(
            "search provider `{provider}` {field} must be an RFC 6901 JSON Pointer"
        ));
    }
    Ok(())
}

fn invalid_json_pointer_escape(token: &str) -> bool {
    let bytes = token.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'~' {
            if index + 1 >= bytes.len() || !matches!(bytes[index + 1], b'0' | b'1') {
                return true;
            }
            index += 2;
        } else {
            index += 1;
        }
    }
    false
}

fn valid_http_header_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn is_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn is_env_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z') | Some(b'_'))
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_loopback_host(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    host.trim_end_matches('.').eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_runtime(text: &str) -> Result<SearchRuntime, String> {
        let file = crate::ProvidersFile::parse(text)?;
        let default = file.default.and_then(|default| default.search);
        Ok(SearchRuntime::from_specs(default, file.search_provider))
    }

    #[test]
    fn registry_resolves_default_and_keeps_credentials_out_of_debug() {
        let env_name = format!("QCG_SEARCH_TEST_KEY_{}", std::process::id());
        unsafe { std::env::set_var(&env_name, "search-secret") };
        let runtime = parse_runtime(&format!(
            r#"
[default]
search = "tinyfish-api"

[[search_provider]]
id = "tinyfish-api"
endpoint = "https://api.search.tinyfish.ai"
query_param = "query"
results_pointer = "/results"
snippet_pointer = "/snippet"
api_key_env = "{env_name}"
auth_header = "X-API-Key"
"#
        ))
        .expect("registry should parse");
        let profile = runtime.resolve(None).expect("default should resolve");
        assert_eq!(profile.id, "tinyfish-api");
        assert!(!format!("{profile:?}").contains("search-secret"));
        unsafe { std::env::remove_var(&env_name) };
    }

    #[test]
    fn registry_supports_post_array_queries_and_static_body() {
        let runtime = parse_runtime(
            r#"
[[search_provider]]
id = "parallel-fast"
endpoint = "https://api.parallel.ai/v1/search"
method = "post"
query_param = "search_queries"
query_is_array = true
body = { mode = "fast" }
results_pointer = "/results"
url_pointer = "/url"
title_pointer = "/title"
snippet_pointer = "/excerpts"
"#,
        )
        .expect("registry should parse");
        let profile = runtime
            .resolve(Some("parallel-fast"))
            .expect("profile should resolve");
        assert_eq!(profile.method, SearchMethod::Post);
        assert!(profile.query_is_array);
        assert_eq!(profile.body["mode"], "fast");
    }

    #[test]
    fn registry_rejects_unsafe_or_ambiguous_authentication() {
        let error = parse_runtime(
            r#"
[[search_provider]]
id = "unsafe"
endpoint = "https://search.example.test"
results_pointer = "/results"
api_key_env = "QCG_SEARCH_KEY"
auth_header = "Authorization"
auth_query_param = "api_key"
"#,
        )
        .expect_err("ambiguous authentication should fail");
        assert!(
            error
                .to_string()
                .contains("must pair api_key_env with exactly one")
        );
    }

    #[test]
    fn registry_rejects_static_fields_that_override_runtime_inputs() {
        let error = parse_runtime(
            r#"
[[search_provider]]
id = "ambiguous"
endpoint = "https://search.example.test"
query_param = "q"
query = { q = "fixed" }
results_pointer = "/results"
"#,
        )
        .expect_err("static query must not override the model query");
        assert!(error.contains("must not override query_param"), "{error}");

        let error = parse_runtime(
            r#"
[[search_provider]]
id = "ambiguous"
endpoint = "https://search.example.test"
method = "post"
query_param = "query"
limit_param = "query"
results_pointer = "/results"
"#,
        )
        .expect_err("query and limit parameters must differ");
        assert!(error.contains("must differ from query_param"), "{error}");
    }

    #[test]
    fn missing_registry_has_actionable_guidance() {
        let error = SearchRuntime::unavailable()
            .resolve(Some("missing-search-provider"))
            .expect_err("missing registry should fail");
        assert!(error.contains("--providers"));
        assert!(error.contains("QCG_PROVIDERS"));
    }
}
