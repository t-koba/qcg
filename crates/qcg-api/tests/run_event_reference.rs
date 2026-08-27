#[test]
fn run_event_reference_matches_generated_markdown() {
    let docs_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("docs/run-event-reference.md");
    let docs = std::fs::read_to_string(docs_path).unwrap();
    let start = "<!-- qcg-run-events:start -->";
    let end = "<!-- qcg-run-events:end -->";
    let block = docs
        .split_once(start)
        .and_then(|(_, tail)| tail.split_once(end).map(|(block, _)| block))
        .expect("run event reference must contain generated markers");
    let expected = format!("\n{}", qcg_api::run_event_reference_markdown());
    assert_eq!(block.trim_end(), expected.trim_end());
}
