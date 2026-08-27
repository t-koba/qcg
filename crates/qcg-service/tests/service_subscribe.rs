use camino::Utf8PathBuf;
use futures_util::StreamExt as _;
use qcg_service::LocalQcgService;
use qcg_service::run_meta_dir;

#[tokio::test]
async fn subscribe_replays_history_and_interruption_for_orphaned_run() {
    let run_id = format!("history-{}", std::process::id());
    let runs_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let run_dir = runs_dir.join(&run_id);
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    std::fs::write(
        run_meta_dir(&run_dir).join("journal.jsonl"),
        r#"{"t":"run_started","seq":1,"ts":"2026-01-01T00:00:00Z","run_id":"history","generator":"demo","generator_path":"demo","contract_sha256":"abc","inputs":{},"resource_hashes":[],"qcg":"0.1.0","schema_version":1}"#
            .to_string()
            + "\n",
    )
    .unwrap();

    let service = LocalQcgService::new(Utf8PathBuf::new(), runs_dir, None)
        .expect("service should initialize");
    let mut events = service.subscribe(run_id).await.unwrap();
    let first = events.next().await.unwrap();
    assert_eq!(first.kind, "run_started");
    let started = first
        .data
        .run_started()
        .expect("run_started data should be typed");
    assert_eq!(started.generator, "demo");
    assert_eq!(started.contract_sha256, "abc");
    let interrupted = events
        .next()
        .await
        .expect("interruption should be replayed");
    assert_eq!(interrupted.kind, "run_interrupted");
    assert!(events.next().await.is_none());
}
