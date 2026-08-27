use async_trait::async_trait;
use camino::Utf8PathBuf;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{
    Engine, RunOptions, StepContext, StepError, StepExecutor, StepOutcome, StepRegistry,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;

struct DemoEchoStep;

#[async_trait]
impl StepExecutor for DemoEchoStep {
    fn type_id(&self) -> &'static str {
        "demo.echo"
    }

    fn params_schema(&self) -> Option<serde_json::Value> {
        Some(json!({
            "type": "object",
            "required": ["message", "destination"],
            "properties": {
                "message": { "type": "string" },
                "destination": { "type": "string" }
            }
        }))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        if node.param_str("message").unwrap_or_default().is_empty() {
            return Err(StepError::failed(&node.id, "message is required"));
        }
        if node.param_str("destination").unwrap_or_default().is_empty() {
            return Err(StepError::failed(&node.id, "destination is required"));
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let output_file =
            ctx.render_inline(node, node.param_str("destination").expect("validated"))?;
        let content = ctx.render_inline(node, node.param_str("message").expect("validated"))?;
        let target = ctx
            .run
            .fs
            .resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        tokio::fs::write(&target, &content).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "echoed": content })),
            files: vec![target],
        })
    }
}

#[tokio::test]
async fn external_step_executor_registers_without_contract_or_engine_changes() {
    let generator = temp_dir("external-step-generator");
    let output = temp_dir("external-step-output");
    let _ = fs::remove_dir_all(&generator);
    let _ = fs::remove_dir_all(&output);
    fs::create_dir_all(&generator).expect("generator dir should be creatable");
    fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "external-step"
name = "External Step"
version = "0.1.0"
qcg_version = "^0.1"
description = "Exercises an externally registered step type."

[[inputs.stages]]
id = "basic"

  [[inputs.stages.fields]]
  id = "name"
  type = "string"
  required = true

[permissions]

[[flow]]
id = "echo"
type = "demo.echo"
artifact = { label = "Result", required = true }

[flow.params]
destination = "result.txt"
message = "hello {{ inputs.name }}"
"#,
    )
    .expect("manifest should be writable");

    let contract = Contract::load(&generator).expect("contract should load unknown step type");
    let mut registry = StepRegistry::new();
    registry.register(DemoEchoStep);
    let manifest = Engine::new(registry)
        .run(
            contract,
            BTreeMap::from([("name".to_string(), json!("qcg"))]),
            RunOptions {
                output_dir: output.clone(),
                json_events: false,
                event_sender: None,
                interactive: false,
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                max_total_steps: RunOptions::default_max_total_steps(),
                max_parallel_steps: RunOptions::default_max_parallel_steps(),
                llm_provider: None,
                llm_seed_override: None,
                cancellation: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .expect("external step should run");

    assert_eq!(
        fs::read_to_string(output.join("result.txt")).unwrap(),
        "hello qcg"
    );
    assert!(
        manifest
            .artifacts
            .iter()
            .any(|artifact| artifact.path == "result.txt")
    );
}

fn temp_dir(name: &str) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-integration-{}-{name}", std::process::id())),
    )
    .expect("temporary path should be UTF-8")
}
