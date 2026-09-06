use camino::Utf8PathBuf;
use futures_util::StreamExt as _;
use qcg_api::RunStatus;
use qcg_contract::Contract;
use qcg_engine::JournalWriter;
use qcg_service::LocalQcgService;
use qcg_service::run_meta_dir;
use serde_json::json;

#[tokio::test]
async fn subscribe_replays_history_and_resumes_an_orphaned_run() {
    let run_id = format!("history-{}", std::process::id());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let generators_dir = root.join("generators");
    let generator_dir = generators_dir.join("demo");
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "resumed"
"#,
    )
    .unwrap();
    let contract = Contract::load(&generator_dir).unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap()
    .event(
        "run_started",
        json!({
            "generator": "demo",
            "generator_path": generator_dir,
            "contract_sha256": contract.sha256,
            "inputs": {},
            "resource_hashes": [],
            "qcg": "0.1.0",
            "schema_version": 1,
        }),
    )
    .unwrap();

    let service =
        LocalQcgService::new(generators_dir, runs_dir, None).expect("service should initialize");
    assert_eq!(
        service.snapshot(run_id.clone()).await.unwrap().state,
        RunStatus::Queued
    );
    let mut events = service.subscribe(run_id.clone()).await.unwrap();
    let first = events.next().await.unwrap();
    assert_eq!(first.kind, "run_started");
    let started = first
        .data
        .run_started()
        .expect("run_started data should be typed");
    assert_eq!(started.generator, "demo");
    assert_eq!(started.contract_sha256, contract.sha256);
    service.resume_recovered_runs().await;
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = events.next().await.expect("resumed event stream closed");
            match event.kind.as_str() {
                "run_finished" => break event,
                "run_error" | "run_canceled" => {
                    panic!("resumed run terminated unexpectedly: {event:?}")
                }
                _ => {}
            }
        }
    })
    .await
    .expect("resumed run should finish");
    assert_eq!(terminal.kind, "run_finished");
    assert_eq!(
        service.snapshot(run_id).await.unwrap().state,
        RunStatus::Succeeded
    );
    assert_eq!(
        std::fs::read_to_string(run_dir.join("workspace/result.txt")).unwrap(),
        "resumed"
    );
    let _ = std::fs::remove_dir_all(root);
}
