use super::support::*;
use api::{RunStatus, StartRun};
use model::{BudgetExhaustion, FailureCode};
use serde_json::{Value, json};

#[tokio::test]
async fn budget_admission_facts_reach_terminal_journals_and_api_snapshots() {
    for resource in ["tokens", "cost_microusd"] {
        for repair in [false, true] {
            let root = temp_run_dir("budget-failure-report");
            let _guard = TempGuard(root.clone());
            let package = root.join("generators/budget");
            std::fs::create_dir_all(&package).unwrap();
            let setup = if repair {
                r#"
[[flow]]
id = "render"
type = "render"
[flow.params]
template = "invalid.j2"
output_file = "invalid.json"
[[flow]]
id = "check"
type = "check.format"
needs = ["render"]
on_fail = { action = "repair", repair = "generate", recheck = "recheck", max_attempts = 1, on_exhausted = { action = "fail" } }
[flow.params]
source = "invalid.json"
content = "json"
[[flow]]
id = "recheck"
type = "check.format"
[flow.params]
source = "invalid.json"
content = "json"
"#
            } else {
                ""
            };
            let budget = if resource == "tokens" {
                "max_tokens = 1"
            } else {
                "max_cost_usd = 0.000001"
            };
            std::fs::write(
                package.join(contract::MANIFEST_FILE),
                format!(
                    r#"
[generator]
id = "budget"
name = "Budget report"
version = "0.1.0"
[budget]
{budget}
{setup}
[[flow]]
id = "generate"
type = "llm.generate"
[flow.params]
prompt = "prompt.j2"
[llm]
max_tokens = 16
temperature = 0.0
[llm.model]
provider = "fake"
model = "fake"
input_cost_per_million_usd = 1.0
output_cost_per_million_usd = 1.0
[permissions]
fs_read = ["workspace"]
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
"#
                ),
            )
            .unwrap();
            std::fs::write(package.join("prompt.j2"), "Produce a short greeting.").unwrap();
            std::fs::write(package.join("invalid.j2"), "not JSON").unwrap();
            let service = test_service(vec![root.join("generators")], root.join("runs"));
            let id = service
                .start_run(StartRun {
                    generator_id: "budget".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let snapshot = wait_for_terminal_snapshot(&service, &id).await;
            assert_eq!(
                snapshot.state,
                RunStatus::Failed,
                "{resource} repair={repair}"
            );
            let path = if repair {
                "check@repair.1/generate"
            } else {
                "generate"
            };
            let detail = snapshot.step_summaries[path].reason.as_ref().unwrap();
            assert_eq!(detail.code, FailureCode::BudgetExceeded);
            let facts = detail.exhaustion.as_ref().unwrap();
            assert_eq!(facts.resource, resource);
            assert_eq!(facts.allowed, 1);
            assert!(facts.used > facts.allowed);
            // Admission refusal precedes provider dispatch and changes no spent usage.
            assert_eq!(snapshot.metrics.as_ref().unwrap().llm_calls, 0);
            assert_eq!(snapshot.metrics.as_ref().unwrap().tokens_total, 0);
            assert_eq!(snapshot.metrics.as_ref().unwrap().cost_microusd, 0);
            let events: Vec<Value> = read_journal_string(&service, id.clone())
                .await
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let terminal = events
                .iter()
                .find(|event| event["t"] == if repair { "run_error" } else { "run_finished" })
                .unwrap();
            if repair {
                assert_eq!(terminal["reason"], json!(detail));
                assert_eq!(terminal["error"], detail.message);
            } else {
                assert!(
                    terminal["failures"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|f| f["failure"] == json!(detail))
                );
            }
            assert_eq!(
                serde_json::from_value::<BudgetExhaustion>(json!(facts)).unwrap(),
                *facts
            );
            let reread = service.snapshot(id.clone()).await.unwrap();
            assert_eq!(reread.step_summaries[path].reason.as_ref(), Some(detail));
            service.shutdown_active_runs().await.unwrap();
            drop(service);
            // The terminal snapshot may precede the engine task's return;
            // wait on the real store lock instead of racing its final owner drop.
            wait_for_store_lock_release(root.join("runs")).await;
            let reopened = test_service(vec![root.join("generators")], root.join("runs"));
            let restored = reopened.snapshot(id).await.unwrap();
            assert_eq!(restored.step_summaries[path].reason.as_ref(), Some(detail));
            reopened.shutdown_active_runs().await.unwrap();
        }
    }
}
