use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::require;

pub(crate) struct ReadAnchoredStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadAnchoredParams {
    path: String,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    limit: Option<u64>,
}

#[async_trait]
impl StepExecutor for ReadAnchoredStep {
    fn type_id(&self) -> &'static str {
        "read_anchored"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["path"],
            json!({
                "path": string_schema(),
                "offset": { "type": "integer", "minimum": 1 },
                "limit": { "type": "integer", "minimum": 1, "maximum": 2000 },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = read_anchored_params(node)?;
        if let Some(limit) = params.limit
            && (limit == 0 || limit > qcg_policy::MAX_ANCHORED_READ_LINES as u64)
        {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "read_anchored limit must be from 1 through {}",
                    qcg_policy::MAX_ANCHORED_READ_LINES
                ),
            ));
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = read_anchored_params(node)?;
        let path = ctx.render_inline(node, &params.path)?;
        let target = ctx
            .run
            .fs
            .resolve_read(&path)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let offset = params.offset.unwrap_or(1).max(1) as usize;
        let limit = params.limit.unwrap_or(200) as usize;
        if limit == 0 || limit > qcg_policy::MAX_ANCHORED_READ_LINES {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "read_anchored limit must be from 1 through {}",
                    qcg_policy::MAX_ANCHORED_READ_LINES
                ),
            ));
        }
        let max_bytes = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        let snapshot = ctx
            .run
            .fs
            .read_anchored(&target, offset, limit, max_bytes)
            .map_err(|error| StepError::from_gateway(&node.id, error))?;
        let lines: Vec<Value> = snapshot
            .lines
            .iter()
            .map(|line| json!({"anchor": line.anchor(), "text": line.text}))
            .collect();
        Ok(StepOutcome::Success {
            output: Some(json!({
                "file": path,
                "base_sha256": snapshot.base_sha256,
                "total_lines": snapshot.total_lines,
                "lines": lines,
            })),
            files: vec![],
        })
    }
}

fn read_anchored_params(node: &NodeDef) -> Result<ReadAnchoredParams, StepError> {
    let params: ReadAnchoredParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid read_anchored params: {error}"))
    })?;
    require(node, Some(&params.path), "path")?;
    Ok(params)
}

pub(crate) struct PatchStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchEditParam {
    op: String,
    anchor: String,
    #[serde(default)]
    lines: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchParams {
    target: String,
    // Required compare-and-swap pin: every patch binds the exact snapshot
    // it was computed against, so two sequential or racing patches to one
    // file fail closed on the loser instead of losing updates. The agent
    // `fs.patch` tool enforces the same pin; the step must not accept less.
    expected_base_sha256: String,
    edits: Vec<PatchEditParam>,
}

#[async_trait]
impl StepExecutor for PatchStep {
    fn type_id(&self) -> &'static str {
        "patch"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["target", "expected_base_sha256", "edits"],
            json!({
                "target": string_schema(),
                "expected_base_sha256": string_schema(),
                "edits": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 128,
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["op", "anchor"],
                        "properties": {
                            "op": { "type": "string", "enum": ["replace", "append", "prepend"] },
                            "anchor": string_schema(),
                            "lines": { "type": "array", "items": { "type": "string" } },
                        }
                    }
                },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = patch_params(node)?;
        if params.edits.is_empty() || params.edits.len() > qcg_policy::MAX_PATCH_EDITS {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "patch edits must contain from 1 through {} items",
                    qcg_policy::MAX_PATCH_EDITS
                ),
            ));
        }
        for edit in &params.edits {
            if !matches!(edit.op.as_str(), "replace" | "append" | "prepend")
                && !edit.op.contains("{{")
            {
                return Err(StepError::failed(
                    &node.id,
                    "patch edit op must be replace, append, or prepend",
                ));
            }
            if edit.anchor.trim().is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "patch edit anchor must not be empty",
                ));
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = patch_params(node)?;
        let target_name = ctx.render_inline(node, &params.target)?;
        let expected = ctx.render_inline(node, &params.expected_base_sha256)?;
        let mut raw = Vec::with_capacity(params.edits.len());
        for edit in &params.edits {
            let op = ctx.render_inline(node, &edit.op)?;
            let anchor = ctx.render_inline(node, &edit.anchor)?;
            let mut lines = Vec::with_capacity(edit.lines.len());
            for line in &edit.lines {
                lines.push(ctx.render_inline(node, line)?);
            }
            raw.push(qcg_fs::RawPatchEdit { op, anchor, lines });
        }
        let edits = qcg_fs::parse_edits(&raw)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let limits = patch_limits(ctx);
        let target = ctx
            .run
            .fs
            .resolve_write(&target_name)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        match ctx
            .run
            .fs
            .apply_anchored_patch(&target, Some(&expected), edits, limits)
            .await
        {
            Ok(outcome) => Ok(StepOutcome::Success {
                output: Some(json!({
                    "file": target_name,
                    "applied": outcome.applied,
                    "base_sha256": outcome.new_base_sha256,
                })),
                files: vec![target],
            }),
            Err(qcg_engine::GatewayError::AnchoredPatch(inner)) => {
                Err(StepError::failed(&node.id, patch_error_message(&inner)))
            }
            Err(error) => Err(StepError::from_gateway(&node.id, error)),
        }
    }
}

fn patch_params(node: &NodeDef) -> Result<PatchParams, StepError> {
    let params: PatchParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid patch params: {error}")))?;
    require(node, Some(&params.target), "target")?;
    require(
        node,
        Some(params.expected_base_sha256.as_str()),
        "expected_base_sha256",
    )?;
    if params.edits.is_empty() {
        return Err(StepError::failed(
            &node.id,
            "patch requires at least one edit",
        ));
    }
    Ok(params)
}

fn patch_limits(ctx: &StepContext<'_>) -> qcg_fs::PatchLimits {
    let runtime = &ctx.run.contract.manifest.runtime;
    qcg_fs::PatchLimits {
        max_edits: runtime
            .patch_hunks_limit
            .unwrap_or(qcg_policy::DEFAULT_PATCH_EDITS),
        max_patch_bytes: runtime
            .patch_bytes_limit
            .unwrap_or(qcg_policy::DEFAULT_PATCH_BYTES),
        max_result_bytes: runtime.output_file_limit_bytes.unwrap_or(
            runtime
                .output_total_limit_bytes
                .unwrap_or(qcg_policy::DEFAULT_LLM_CONTEXT_LIMIT_BYTES),
        ),
    }
}

fn patch_error_message(error: &qcg_fs::AnchoredPatchError) -> String {
    use qcg_fs::AnchoredPatchError as Error;
    match error {
        Error::AnchorStale {
            anchor,
            remaps,
            current_base,
        } => {
            let hints: Vec<String> = remaps
                .iter()
                .map(|remap| format!("{} -> {}", remap.stale_anchor, remap.current_anchor))
                .collect();
            format!(
                "patch anchor `{anchor}` is stale (base `{current_base}`); remaps: {}",
                hints.join(", ")
            )
        }
        other => format!("patch rejected: {other}"),
    }
}
