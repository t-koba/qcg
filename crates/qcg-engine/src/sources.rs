use qcg_types::credential_like_name;
use serde_json::{Value, json};
use url::Url;

pub const TOOL_EVENT_SOURCE_LIMIT: usize = 64;
pub const TOOL_EVENT_SOURCE_SCAN_BYTES: usize = 256 * 1024;
pub const TOOL_EVENT_SOURCE_SCAN_NODES: usize = 8_192;
pub const TOOL_EVENT_SOURCE_SCAN_DEPTH: usize = 64;

fn tool_source_url_regex() -> &'static regex::Regex {
    static URLS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    URLS.get_or_init(|| {
        regex::Regex::new(r#"https?://[^\s<>\"']+"#).expect("tool source URL regex must compile")
    })
}

/// Extracts bounded, public source references from a structured tool result.
/// Explicit resource URL fields are handled together with URLs mentioned in
/// text because both forms are used by MCP servers.
pub fn tool_call_sources(value: &Value) -> Vec<Value> {
    let mut explicit_sources = std::collections::BTreeMap::<String, Option<String>>::new();
    let mut text_sources = std::collections::BTreeMap::<String, Option<String>>::new();
    let mut stack = vec![(value, None::<String>, 1_usize)];
    let mut nodes = 0_usize;
    let mut string_bytes = 0_usize;
    while let Some((value, inherited_title, depth)) = stack.pop() {
        if nodes >= TOOL_EVENT_SOURCE_SCAN_NODES || depth > TOOL_EVENT_SOURCE_SCAN_DEPTH {
            continue;
        }
        nodes = nodes.saturating_add(1);
        match value {
            Value::Object(object) => {
                let object_title = object
                    .get("title")
                    .or_else(|| object.get("name"))
                    .and_then(Value::as_str)
                    .filter(|title| !title.trim().is_empty())
                    .map(|title| title.chars().take(512).collect::<String>())
                    .or(inherited_title);
                for (key, value) in object.iter().rev() {
                    if stack.len().saturating_add(nodes) >= TOOL_EVENT_SOURCE_SCAN_NODES {
                        break;
                    }
                    if matches!(key.as_str(), "url" | "uri" | "link" | "href" | "source")
                        && let Some(url) = value.as_str().and_then(public_source_url)
                        && explicit_sources.len() < TOOL_EVENT_SOURCE_LIMIT
                    {
                        explicit_sources
                            .entry(url)
                            .or_insert_with(|| object_title.clone());
                    }
                    stack.push((value, object_title.clone(), depth + 1));
                }
            }
            Value::Array(values) => {
                let remaining_nodes =
                    TOOL_EVENT_SOURCE_SCAN_NODES.saturating_sub(stack.len().saturating_add(nodes));
                stack.extend(
                    values
                        .iter()
                        .rev()
                        .take(remaining_nodes)
                        .map(|value| (value, inherited_title.clone(), depth + 1)),
                );
            }
            Value::String(text) => {
                let remaining = TOOL_EVENT_SOURCE_SCAN_BYTES.saturating_sub(string_bytes);
                if remaining == 0 {
                    continue;
                }
                let scanned = utf8_head(text, remaining);
                string_bytes = string_bytes.saturating_add(scanned.len());
                for candidate in tool_source_url_regex()
                    .find_iter(scanned)
                    .map(|matched| matched.as_str())
                {
                    if let Some(url) = public_source_url(candidate)
                        && text_sources.len() < TOOL_EVENT_SOURCE_LIMIT
                    {
                        text_sources
                            .entry(url)
                            .or_insert_with(|| inherited_title.clone());
                    }
                }
            }
            _ => {}
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    explicit_sources
        .into_iter()
        .chain(text_sources)
        .filter_map(|(url, title)| {
            seen.insert(url.clone()).then_some(json!({
                "url": url,
                "title": title,
            }))
        })
        .take(TOOL_EVENT_SOURCE_LIMIT)
        .collect()
}

fn public_source_url(candidate: &str) -> Option<String> {
    let candidate = candidate.trim_end_matches(['.', ',', ';', ':', ')', ']', '}']);
    if candidate.len() > 4096 {
        return None;
    }
    let mut url = Url::parse(candidate).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let retained = url
        .query_pairs()
        .filter(|(name, _)| !credential_like_name(name.as_ref()))
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !retained.is_empty() {
        url.query_pairs_mut().extend_pairs(retained);
    }
    url.set_fragment(None);
    Some(url.to_string())
}

fn utf8_head(value: &str, limit: usize) -> &str {
    if value.len() <= limit {
        return value;
    }
    let mut end = limit;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_public_query_and_removes_credential_query() {
        let sources = tool_call_sources(&json!({
            "content": [{
                "text": "See https://example.test/search?q=rust&api_key=secret#section"
            }]
        }));
        assert_eq!(
            sources,
            vec![json!({"url": "https://example.test/search?q=rust", "title": null})]
        );
    }

    #[test]
    fn records_explicit_resource_link_sources() {
        let sources = tool_call_sources(&json!({
            "content": [{
                "type": "resource_link",
                "name": "Reference",
                "uri": "https://example.test/docs?page=2"
            }]
        }));
        assert_eq!(
            sources,
            vec![json!({"url": "https://example.test/docs?page=2", "title": "Reference"})]
        );
    }
}
