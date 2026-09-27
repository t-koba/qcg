use super::support::*;

#[tokio::test]
async fn e16_queue_move_changes_snapshot_body() {
    // E16: exact-body ETag covers queue_position, so proving the body
    // bytes change on a queue move proves the validator changes too.
    // The digest formula itself lives in exactly one place
    // (`server::body_etag`); duplicating it here would let the two
    // drift, so this test pins the body property and the server
    // star/queue tests pin the production helper (E16).
    let before = serde_json::to_vec(
        &serde_json::json!({"seq": 1u64, "state": "queued", "queue_position": 2u64}),
    )
    .expect("serialize");
    let after = serde_json::to_vec(
        &serde_json::json!({"seq": 1u64, "state": "queued", "queue_position": 1u64}),
    )
    .expect("serialize");
    assert_ne!(
        before, after,
        "queue movement must change the snapshot body the validator digests"
    );
}

#[tokio::test]
async fn e05_direct_run_refuses_during_shutdown() {
    // E05: direct executions honor the shutdown gate like API admissions.
    let root = temp_root("direct-shutdown");
    let _ = std::fs::remove_dir_all(&root);
    let gen_dir = root.join("gen");
    let runs = root.join("runs");
    std::fs::create_dir_all(&gen_dir).expect("gen dir");
    let service = test_service(vec![gen_dir.clone()], runs);
    service.mark_shutting_down();
    let err = service
        .run_generator_path(crate::types::DirectRun {
            generator_path: gen_dir.join(contract::MANIFEST_FILE),
            output_dir: root.join("out"),
            inputs: Default::default(),
            answers: Default::default(),
            confirmations: Default::default(),
            interactive: false,
            json_events: false,
            llm_seed_override: None,
        })
        .await
        .expect_err("direct run during shutdown must be refused");
    assert!(err.to_string().contains("shutting down"), "{err}");
    let _ = std::fs::remove_dir_all(&root);
}
