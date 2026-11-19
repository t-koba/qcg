//! Test-only probe for continuation tests: fails until it has evidence of a
//! prior execution of the same node, then succeeds. It reads no runtime
//! inputs and writes nothing, so it can be declared continuation-safe without
//! changing any production step or safety rule.

use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{
    ContinuationReadCoverage, ContinuationSafety, StepContext, StepError, StepExecutor,
    StepOutcome, step_params,
};
use model::Finding;
use serde::Deserialize;
use serde_json::json;

pub struct RecoverProbeStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoverProbeParams {
    attempts_before_success: u64,
}

fn probe_params(node: &NodeDef) -> Result<RecoverProbeParams, StepError> {
    step_params(node, "test.recover_probe")
}

#[async_trait]
impl StepExecutor for RecoverProbeStep {
    engine::step_type_id!("test.recover_probe");

    fn continuation_semantics_version(&self) -> Option<&'static str> {
        Some("1")
    }

    fn continuation_safety(&self, _node: &NodeDef) -> ContinuationSafety {
        ContinuationSafety::ReadOnly
    }

    fn continuation_read_coverage(&self, _node: &NodeDef) -> ContinuationReadCoverage {
        ContinuationReadCoverage::Complete
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        probe_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = probe_params(node)?;
        // Journal state is engine-owned execution history, not a runtime
        // input: it carries no path, value or external read to capture.
        let prior = ctx
            .journal
            .state()
            .node_executions
            .get(&node.id)
            .copied()
            .unwrap_or(0);
        if prior < params.attempts_before_success {
            return Ok(StepOutcome::check_failed(vec![Finding::error(
                format!("probe failed with {prior} prior attempts"),
                None,
                None,
            )]));
        }
        Ok(StepOutcome::success(Some(json!({
            "status": "pass",
            "prior_attempts": prior,
        }))))
    }
}
