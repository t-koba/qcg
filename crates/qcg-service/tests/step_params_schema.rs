use qcg_service::{built_in_step_param_schemas, step_param_schemas_markdown};

#[test]
fn built_in_registry_exposes_parameter_schemas_for_all_steps() {
    let schemas = built_in_step_param_schemas();
    let expected = [
        "ask_user",
        "check.command",
        "check.container",
        "check.contract",
        "check.format",
        "check.schema",
        "check.tool",
        "command",
        "copy",
        "fail",
        "foreach",
        "http",
        "llm.agent",
        "llm.choose",
        "llm.fill",
        "llm.generate",
        "llm.repair",
        "mcp.call",
        "render",
        "transform",
        "write",
    ];
    assert_eq!(schemas.len(), expected.len());
    for step_type in expected {
        let schema = schemas
            .get(step_type)
            .unwrap_or_else(|| panic!("missing schema for {step_type}"));
        assert_eq!(schema["type"], "object");
        assert!(
            schema.get("properties").is_some(),
            "schema for {step_type} must describe properties"
        );
    }
}

#[test]
fn contract_reference_step_schema_block_is_generated_from_registry() {
    let docs_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("docs/contract-reference.md");
    let docs = std::fs::read_to_string(docs_path).unwrap();
    let start = "<!-- qcg-step-schemas:start -->";
    let end = "<!-- qcg-step-schemas:end -->";
    let block = docs
        .split_once(start)
        .and_then(|(_, tail)| tail.split_once(end).map(|(block, _)| block))
        .expect("contract reference must contain step schema markers");
    let expected = format!("\n{}", step_param_schemas_markdown().unwrap());
    assert_eq!(block.trim_end(), expected.trim_end());
}
