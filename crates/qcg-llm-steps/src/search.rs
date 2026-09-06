use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{Contract, NodeDef, ToolDecl};
use qcg_engine::{HttpGateway, HttpRequest, SecretStore, StepError};
use qcg_llm::{SearchMethod, SearchProfile, SearchRuntime};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use url::Url;

pub(crate) fn validate_web_search_tool(
    node: &NodeDef,
    contract: &Contract,
    search_runtime: &SearchRuntime,
    tool: &ToolDecl,
) -> Result<(), StepError> {
    let ToolDecl::WebSearch {
        name,
        provider,
        max_results,
        max_calls,
        ..
    } = tool
    else {
        unreachable!("web search validation requires a web.search tool")
    };
    let profile = search_runtime
        .resolve(provider.as_deref())
        .map_err(|error| StepError::failed(&node.id, error))?;
    let host = profile.host().ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!("search provider `{}` endpoint requires a host", profile.id),
        )
    })?;
    if !contract
        .manifest
        .permissions
        .network
        .iter()
        .any(|allowed| allowed == host)
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "tool `{name}` search provider `{}` host `{host}` is not allowed by permissions.network",
                profile.id
            ),
        ));
    }
    if *max_results == 0 {
        return Err(StepError::failed(
            &node.id,
            format!("tool `{name}` max_results must be greater than zero"),
        ));
    }
    if *max_calls == 0 {
        return Err(StepError::failed(
            &node.id,
            format!("tool `{name}` max_calls must be greater than zero"),
        ));
    }
    Ok(())
}

pub(crate) async fn execute_web_search(
    http: &HttpGateway,
    secrets: &SecretStore,
    search_runtime: &SearchRuntime,
    node: &NodeDef,
    tool: &ToolDecl,
    args: &Value,
) -> Result<Value, StepError> {
    let ToolDecl::WebSearch {
        provider,
        max_results,
        ..
    } = tool
    else {
        unreachable!("web search execution requires a web.search tool")
    };
    let profile = search_runtime
        .resolve(provider.as_deref())
        .map_err(|error| StepError::failed(&node.id, error))?;
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .ok_or_else(|| StepError::failed(&node.id, "web.search tool requires a non-empty query"))?;
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map(|limit| limit as usize)
        .unwrap_or(*max_results);
    let mut url = profile.endpoint.clone().ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!(
                "search provider `{}` endpoint is not configured",
                profile.id
            ),
        )
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in &profile.query {
            pairs.append_pair(key, value);
        }
        if profile.method == SearchMethod::Get {
            pairs.append_pair(&profile.query_param, query);
            if let Some(limit_param) = profile.limit_param.as_deref() {
                pairs.append_pair(limit_param, &limit.to_string());
            }
        }
    }
    for value in profile.headers.values() {
        secrets.assert_absent(value).map_err(|error| {
            StepError::failed(
                &node.id,
                format!(
                    "search provider `{}` static header is invalid: {error}",
                    profile.id
                ),
            )
        })?;
    }
    let credential = profile
        .credential()
        .map_err(|error| StepError::failed(&node.id, error))?;
    if let Some(value) = credential.as_deref()
        && (query.contains(value)
            || profile
                .endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint.as_str().contains(value))
            || profile
                .headers
                .values()
                .chain(profile.query.values())
                .any(|configured| configured.contains(value))
            || profile
                .body
                .values()
                .any(|configured| configured.to_string().contains(value)))
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "search provider `{}` credential must not appear in static configuration",
                profile.id
            ),
        ));
    }
    let mut headers = profile.headers.clone();
    let mut sensitive_query = BTreeMap::new();
    if let Some(value) = credential.as_deref() {
        if let Some(auth_header) = profile.auth_header.as_deref() {
            headers.insert(
                auth_header.to_string(),
                format!("{}{value}", profile.auth_prefix),
            );
        } else if let Some(auth_query_param) = profile.auth_query_param.as_deref() {
            sensitive_query.insert(auth_query_param.to_string(), value.to_string());
        }
    }
    let body = if profile.method == SearchMethod::Post {
        let mut body = serde_json::Map::from_iter(profile.body.clone());
        body.insert(
            profile.query_param.clone(),
            if profile.query_is_array {
                Value::Array(vec![Value::String(query.to_string())])
            } else {
                Value::String(query.to_string())
            },
        );
        if let Some(limit_param) = profile.limit_param.as_deref() {
            body.insert(limit_param.to_string(), Value::from(limit as u64));
        }
        let value = Value::Object(body);
        secrets.assert_absent(&value.to_string()).map_err(|error| {
            StepError::failed(
                &node.id,
                format!(
                    "search provider `{}` static body is invalid: {error}",
                    profile.id
                ),
            )
        })?;
        headers
            .entry("Content-Type".into())
            .or_insert_with(|| "application/json".into());
        Some(serde_json::to_string(&value)?)
    } else {
        None
    };
    let output = http
        .request(HttpRequest {
            method: match profile.method {
                SearchMethod::Get => "GET",
                SearchMethod::Post => "POST",
            }
            .into(),
            url: url.to_string(),
            headers,
            sensitive_query,
            body: body.map(String::into_bytes),
            follow_redirects: false,
        })
        .await
        .map_err(|error| StepError::from_gateway(&node.id, error))?;
    if !(200..300).contains(&output.status) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "search provider `{}` returned HTTP status {}",
                profile.id, output.status
            ),
        ));
    }
    if credential.as_deref().is_some_and(|value| {
        !value.is_empty()
            && output
                .body
                .windows(value.len())
                .any(|window| window == value.as_bytes())
    }) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "search provider `{}` response contained its configured credential",
                profile.id
            ),
        ));
    }
    let body = std::str::from_utf8(&output.body).map_err(|error| {
        StepError::failed(
            &node.id,
            format!(
                "search provider `{}` returned non-UTF-8 JSON bytes: {error}",
                profile.id
            ),
        )
    })?;
    let payload: Value = serde_json::from_str(body).map_err(|error| {
        StepError::failed(
            &node.id,
            format!(
                "search provider `{}` returned invalid JSON: {error}",
                profile.id
            ),
        )
    })?;
    if credential
        .as_deref()
        .is_some_and(|value| value_contains_string(&payload, value))
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "search provider `{}` response contained its configured credential",
                profile.id
            ),
        ));
    }
    normalize_web_search_results(node, query, &payload, profile, limit)
}

pub(crate) fn value_contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(value) => value.contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|value| value_contains_string(value, needle)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains(needle) || value_contains_string(value, needle)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

pub(crate) fn http_body_value(body: &[u8]) -> Value {
    match std::str::from_utf8(body) {
        Ok(text) => Value::String(text.to_owned()),
        Err(_) => json!({
            "encoding": "base64",
            "data": BASE64.encode(body),
        }),
    }
}

pub(crate) fn normalize_web_search_results(
    node: &NodeDef,
    query: &str,
    payload: &Value,
    profile: &SearchProfile,
    limit: usize,
) -> Result<Value, StepError> {
    let raw_results = payload
        .pointer(&profile.results_pointer)
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!(
                    "search provider `{}` results_pointer `{}` is not an array",
                    profile.id, profile.results_pointer
                ),
            )
        })?;
    let mut results = Vec::with_capacity(limit.min(raw_results.len()));
    for (index, result) in raw_results.iter().take(limit).enumerate() {
        let title = required_search_string(result, &profile.title_pointer, index, "title", node)?;
        let result_url = required_search_string(result, &profile.url_pointer, index, "url", node)?;
        let parsed_url = Url::parse(result_url).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("web.search result {index} has an invalid URL: {error}"),
            )
        })?;
        if !matches!(parsed_url.scheme(), "http" | "https")
            || !parsed_url.username().is_empty()
            || parsed_url.password().is_some()
        {
            return Err(StepError::failed(
                &node.id,
                format!("web.search result {index} URL must use HTTP or HTTPS without credentials"),
            ));
        }
        let snippet = profile
            .snippet_pointer
            .as_deref()
            .map(|pointer| optional_search_text(result, pointer, index, "snippet", node))
            .transpose()?
            .flatten();
        validate_search_text_length(node, index, "title", title, 512)?;
        if let Some(snippet) = snippet.as_deref() {
            validate_search_text_length(node, index, "snippet", snippet, 4096)?;
        }
        results.push(json!({
            "rank": index + 1,
            "title": title.trim(),
            "url": parsed_url.to_string(),
            "snippet": snippet.map(|value| value.trim().to_string()),
        }));
    }
    Ok(json!({
        "query": query,
        "content_trust": "untrusted",
        "results": results,
    }))
}

pub(crate) fn required_search_string<'a>(
    result: &'a Value,
    path: &str,
    index: usize,
    field: &str,
    node: &NodeDef,
) -> Result<&'a str, StepError> {
    result
        .pointer(path)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("web.search result {index} is missing its {field} field `{path}`"),
            )
        })
}

pub(crate) fn optional_search_text(
    result: &Value,
    pointer: &str,
    index: usize,
    field: &str,
    node: &NodeDef,
) -> Result<Option<String>, StepError> {
    match result.pointer(pointer) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Array(values))
            if values.iter().all(|value| matches!(value, Value::String(_))) =>
        {
            Ok(Some(
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n"),
            ))
        }
        Some(_) => Err(StepError::failed(
            &node.id,
            format!(
                "web.search result {index} {field} field `{pointer}` must be a string, string array, or null"
            ),
        )),
    }
}

pub(crate) fn validate_search_text_length(
    node: &NodeDef,
    index: usize,
    field: &str,
    value: &str,
    max_chars: usize,
) -> Result<(), StepError> {
    let chars = value.chars().count();
    if chars > max_chars {
        return Err(StepError::failed(
            &node.id,
            format!("web.search result {index} {field} exceeds {max_chars} characters"),
        ));
    }
    Ok(())
}

pub(crate) fn url_host_matches(url: &str, expected_host: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .as_deref()
        == Some(expected_host)
}
