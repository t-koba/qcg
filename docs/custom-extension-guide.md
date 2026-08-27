# Custom Extension Guide

qcg keeps product-specific behavior out of the kernel by routing work through
registries. Hosts can add step executors and apply contract validation rules
without changing the engine scheduler. The stock engine's resource loader set
is currently fixed, as described below.

## Custom StepExecutor

Implement `qcg_engine::StepExecutor` for one step type and register it in a
`StepRegistry`.

```rust
use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepRegistry};
use serde_json::json;

struct ExampleStep;

#[async_trait]
impl StepExecutor for ExampleStep {
    fn type_id(&self) -> &'static str {
        "example.write"
    }

    fn params_schema(&self) -> Option<serde_json::Value> {
        Some(json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["content", "output_file"],
            "properties": {
                "content": { "type": "string" },
                "output_file": { "type": "string" }
            }
        }))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        if node.param_str("output_file").unwrap_or_default().is_empty() {
            return Err(StepError::failed(&node.id, "output_file is required"));
        }
        Ok(())
    }

    async fn execute(&self, ctx: &mut StepContext<'_>, node: &NodeDef) -> Result<StepOutcome, StepError> {
        let output_file = ctx.render_inline(node, node.param_str("output_file").expect("validated"))?;
        let content = ctx.render_inline(node, node.param_str("content").unwrap_or_default())?;
        let target = ctx.run.fs.resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        tokio::fs::write(&target, &content).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "path": output_file })),
            files: vec![target],
        })
    }
}

let mut registry = StepRegistry::new();
registry.register(ExampleStep);
```

Use the integration test at `tests/integration/tests/custom_step.rs` as the
reference fixture. It proves that a third-party step can be registered and run
without editing `qcg-contract`, `qcg-engine`, or the CLI.

## Resource loader boundary

`ResourceLoader` and `ResourceRegistry` are public implementation types, but
the stock `Engine` currently constructs its built-in registry internally and
does not accept an injected resource registry. Therefore external resource
kinds are not a supported extension point. Use the built-in `file`, `dir`,
`skill`, `url`, and `openapi` kinds, or add a custom step executor that stays
inside the existing filesystem and HTTP gateways. Do not claim a custom
resource kind in a contract until the host exposes an explicit registry
injection path.

## Contract Validation Rules

Use `qcg_contract::ContractValidationRule` for cross-cutting contract checks.
Rules should reject invalid configuration explicitly instead of silently
falling back to a weaker behavior. Register the rule on a
`ContractValidator`, call `manifest.validate_with(&validator)`, and complete
that host-side validation before constructing `Engine`; the stock CLI does not
discover third-party rules automatically.
