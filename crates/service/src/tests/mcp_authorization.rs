use super::support::*;
use crate::*;
use api::{AnswerPayload, RunStatus, StartRun};
use serde_json::Value;
use std::collections::BTreeMap;

#[tokio::test]
async fn empty_authorization_answer_requeues_same_run_and_rechecks_before_dispatch() {
    let root = temp_run_dir("mcp-authorization-resume");
    let _guard = TempGuard(root.clone());
    let generators = root.join("generators");
    let package = generators.join("authorization");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "authorization"
name = "Authorization"
version = "0.1.0"

[[flow]]
id = "call"
type = "mcp.call"
[flow.params]
server = "oauth"
tool = "echo"
arguments = {}
side_effects = false
on_authorization_required = "wait"

[permissions]
fs_read = []
fs_write = ["workspace"]
network = ["127.0.0.1"]
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
"#,
    )
    .unwrap();
    let providers_path = root.join("providers.toml");
    std::fs::write(
        &providers_path,
        r#"
[[mcp_server]]
id = "oauth"
transport = "streamable_http"
url = "http://127.0.0.1:1/mcp"
auth = "oauth"
oauth_store = "memory"
allowed_hosts = ["127.0.0.1"]
timeout_seconds = 1
"#,
    )
    .unwrap();
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        root.join("runs"),
        Some(providers_path),
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .unwrap();
    let id = service
        .start_run(StartRun {
            generator_id: "authorization".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = first
        .question
        .expect("run should wait for OAuth authorization");
    assert!(question.fields.is_empty());
    service
        .answer(
            id.clone(),
            question.id.clone(),
            AnswerPayload {
                values: BTreeMap::new(),
            },
        )
        .await
        .expect("zero-field authorization answer should resume the run");

    let second = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let snapshot = service.snapshot(id.clone()).await.unwrap();
            if snapshot.state == RunStatus::Waiting
                && snapshot
                    .question
                    .as_ref()
                    .is_some_and(|next| next.id != question.id)
            {
                return snapshot;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("resumed run should reissue an authorization prerequisite");
    assert_eq!(second.state, RunStatus::Waiting);
    assert_ne!(second.question.unwrap().id, question.id);

    let records = read_journal_string(&service, id.clone())
        .await
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    let authorization = records
        .iter()
        .filter(|record| record["t"] == "mcp_authorization_required")
        .collect::<Vec<_>>();
    assert_eq!(authorization.len(), 2);
    assert!(authorization.iter().all(|record| {
        record["action"] == "wait"
            && record["source"] == "node_parameter"
            && record["phase"] == "connect"
    }));
    assert!(
        records
            .iter()
            .all(|record| record["t"] != "operation_started")
    );
    assert!(records.iter().all(|record| record["t"] != "tool_call"));
}
