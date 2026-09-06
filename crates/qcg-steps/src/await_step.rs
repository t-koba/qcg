use async_trait::async_trait;
use qcg_contract::Contract;
use qcg_contract::NodeDef;
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::params_schema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
pub(crate) struct AwaitStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AwaitParams {
    runs: Vec<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[async_trait]
impl StepExecutor for AwaitStep {
    fn type_id(&self) -> &'static str {
        "await"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["runs"],
            json!({
                "runs": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
                "timeout_secs": { "type": "integer", "minimum": 1 },
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        validate_await_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = validate_await_params(node)?;
        let source = ctx.run.snapshot_source.clone().ok_or_else(|| {
            StepError::failed(
                &node.id,
                "await requires server execution with sibling run visibility",
            )
        })?;
        let started = std::time::Instant::now();
        loop {
            if ctx.run.cancellation.is_cancelled() {
                return Err(StepError::Cancelled);
            }
            let mut states = BTreeMap::new();
            let mut pending = false;
            for run_id in &params.runs {
                match source.run_status(run_id).await {
                    None => {
                        return Err(StepError::failed(
                            &node.id,
                            format!("awaited run `{run_id}` was not found"),
                        ));
                    }
                    Some(state) => {
                        let terminal = state.is_terminal();
                        states.insert(
                            run_id.clone(),
                            json!({
                                "state": serde_json::to_value(state).unwrap_or(Value::Null),
                                "terminal": terminal,
                            }),
                        );
                        pending = pending || !terminal;
                    }
                }
            }
            if !pending {
                return Ok(StepOutcome::Success {
                    output: Some(Value::Object(
                        states.into_iter().collect::<serde_json::Map<_, _>>(),
                    )),
                    files: vec![],
                });
            }
            if params
                .timeout_secs
                .is_some_and(|timeout| started.elapsed() >= std::time::Duration::from_secs(timeout))
            {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "await timed out after {}s",
                        params.timeout_secs.unwrap_or(0)
                    ),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
}

fn validate_await_params(node: &NodeDef) -> Result<AwaitParams, StepError> {
    let params: AwaitParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid await params: {error}")))?;
    if params.runs.is_empty() {
        return Err(StepError::failed(
            &node.id,
            "await requires at least one run id",
        ));
    }
    if params.timeout_secs.is_some_and(|timeout| timeout == 0) {
        return Err(StepError::failed(
            &node.id,
            "await timeout_secs must be at least 1 when set",
        ));
    }
    Ok(params)
}
