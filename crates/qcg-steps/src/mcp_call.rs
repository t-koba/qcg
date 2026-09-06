mod direct;
mod step;
mod types;
mod validate;

pub(crate) use types::McpCallStep;

#[cfg(test)]
mod tests {
    use qcg_engine::tool_call_sources;
    use serde_json::json;

    #[test]
    fn direct_mcp_source_extraction_keeps_text_and_resource_sources() {
        let result = json!({
            "content": [
                {
                    "type": "text",
                    "text": "Research source: https://example.test/search?q=rust&token=secret#part"
                },
                {
                    "type": "resource_link",
                    "name": "Reference",
                    "uri": "https://example.test/docs?page=2"
                }
            ]
        });
        let sources = tool_call_sources(&result);
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0]["url"], "https://example.test/docs?page=2");
        assert_eq!(sources[0]["title"], "Reference");
        assert_eq!(sources[1]["url"], "https://example.test/search?q=rust");
    }
}
