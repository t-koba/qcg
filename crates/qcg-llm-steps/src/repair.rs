use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{ResultExt, StepContext, StepError, StepExecutor, StepOutcome};
use qcg_llm::LlmRuntime;
use qcg_policy::{params_schema, string_schema};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::agent_runtime::{parse_patch_edits, resolve_patch_limits};
use crate::completion::complete_llm;
use crate::policy::{llm_params, require_prompt};
use crate::prompting::{
    parse_llm_json, read_repair_source, render_repair_patch_prompt, render_repair_prompt_with,
    response_text,
};
use crate::request::build_request;
use crate::schemas::llm_common_properties;
use crate::validation::validate_llm_node;

pub(crate) struct LlmRepairStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}
#[async_trait]
impl StepExecutor for LlmRepairStep {
    fn type_id(&self) -> &'static str {
        "llm.repair"
    }

    fn traits(&self) -> qcg_engine::StepTraits {
        qcg_engine::StepTraits::parallel()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["prompt"],
            llm_common_properties(json!({
                "source": string_schema(),
                "target": string_schema(),
                "mode": { "type": "string", "enum": ["text", "patch"] },
            })),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = llm_params(node)?;
        validate_llm_node(node, contract, &self.runtime, false, false)?;
        require_prompt(node, &params)?;
        let mode = repair_mode(node)?;
        // Patch mode edits the shown snapshot, so it is meaningless
        // without a source to anchor on and a target to receive the
        // result. Both are policy requirements enforced here, never
        // defaulted silently.
        if mode == RepairMode::Patch {
            if params.source.is_none() {
                return Err(StepError::failed(
                    &node.id,
                    "llm.repair patch mode requires source",
                ));
            }
            if params.target.is_none() && params.output_file.is_none() {
                return Err(StepError::failed(
                    &node.id,
                    "llm.repair patch mode requires target or output_file",
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
        // Single snapshot read: the shown text feeds the prompt and the
        // base binds the write-back below, in both modes.
        let snapshot = read_repair_source(ctx, node)?;
        match repair_mode(node)? {
            RepairMode::Text => self.execute_text(ctx, node, snapshot.as_ref()).await,
            RepairMode::Patch => {
                let snapshot = snapshot.ok_or_else(|| {
                    StepError::failed(&node.id, "llm.repair patch mode requires source")
                })?;
                self.execute_patch(ctx, node, &snapshot).await
            }
        }
    }
}

/// Policy-selected repair output contract. `text` rewrites the whole file
/// in one shot (best for small files); `patch` constrains the model to
/// anchored edits (best for large files where unrelated churn must not
/// spread). The mode is declared, never inferred, and never falls back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairMode {
    Text,
    Patch,
}

fn repair_mode(node: &NodeDef) -> Result<RepairMode, StepError> {
    let params = llm_params(node)?;
    match params.mode.as_deref() {
        None | Some("text") => Ok(RepairMode::Text),
        Some("patch") => Ok(RepairMode::Patch),
        Some(other) => Err(StepError::failed(
            &node.id,
            format!("llm.repair mode `{other}` must be text or patch"),
        )),
    }
}

/// Normalizes declared repair paths for self-repair detection. `./x` and
/// `x` name the same workspace file, so the equality check must fold
/// leading `./` segments instead of letting spelling pick the semantic
/// (base-verified write-back versus declared overwrite). Anything beyond
/// leading `./` keeps declared semantics.
fn normalize_repair_path(path: &str) -> &str {
    let mut rest = path;
    while let Some(stripped) = rest.strip_prefix("./") {
        rest = stripped;
    }
    rest
}

impl LlmRepairStep {
    async fn execute_text(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
        snapshot: Option<&crate::prompting::RepairSourceSnapshot>,
    ) -> Result<StepOutcome, StepError> {
        let prompt = render_repair_prompt_with(ctx, node, snapshot)?;
        let request = build_request(ctx, node, &self.runtime, prompt, None)?;
        let response = complete_llm(ctx, node, request, |_| json!({ "repair": true })).await?;
        let text = response_text(response.content)?;

        let mut files = Vec::new();
        let params = llm_params(node)?;
        let output_path = params.target.as_ref().or(params.output_file.as_ref());
        if let Some(output_path) = output_path {
            let output_path = ctx.render_inline(node, output_path)?;
            let target = ctx.run.fs.resolve_write(&output_path).step_err(&node.id)?;
            // Self-repair writes back the file the model was shown. Verify
            // the snapshot is still current before replacing it: a parallel
            // `foreach` iteration or any other writer racing this node must
            // surface as an explicit failure, never as a silent overwrite.
            // The verify-plus-write holds the process-wide per-file
            // exclusion so the check cannot be invalidated between read and
            // commit by an in-process writer. Cross-file repair
            // (`source != target`) keeps the declared overwrite semantic;
            // the model never saw the target there.
            // Paths compare on normalized spelling so `./x` and `x` cannot
            // slip past the base check as a fake cross-file repair.
            let _guard = qcg_engine::lock_patch_paths(&[target.as_path()]).await;
            if let Some(snapshot) = &snapshot
                && normalize_repair_path(&snapshot.path) == normalize_repair_path(&output_path)
            {
                verify_repair_base(ctx, node, &target, snapshot)?;
            }
            ctx.run
                .fs
                .write_file_atomic(&target, text.as_bytes())
                .await
                .map_err(|error| StepError::from_gateway(&node.id, error))?;
            files.push(target);
        }
        Ok(StepOutcome::Success {
            output: Some(json!({ "text": text })),
            files,
        })
    }

    /// Single-shot anchored repair. The model sees anchored lines and must
    /// return `{"edits": [...]}`; the edits apply through the same anchored
    /// mechanism as `patch` steps and agent `fs.patch` calls. Malformed
    /// output and stale snapshots fail the node explicitly: there is no
    /// silent fallback to full rewrite, in either direction.
    async fn execute_patch(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
        snapshot: &crate::prompting::RepairSourceSnapshot,
    ) -> Result<StepOutcome, StepError> {
        let prompt = render_repair_patch_prompt(ctx, node, snapshot)?;
        let request = build_request(ctx, node, &self.runtime, prompt, None)?;
        let response = complete_llm(ctx, node, request, |_| json!({ "repair": true })).await?;
        let text = response_text(response.content)?;
        let value = parse_llm_json(&text).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("repair patch output was not JSON: {error}"),
            )
        })?;
        let raw = value
            .get("edits")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                StepError::failed(&node.id, "repair patch output requires an edits array")
            })?;
        let edits = parse_patch_edits(node, raw)?;
        let limits = resolve_patch_limits(ctx, node)?;
        let params = llm_params(node)?;
        let output_path = params
            .target
            .as_ref()
            .or(params.output_file.as_ref())
            .ok_or_else(|| {
                StepError::failed(
                    &node.id,
                    "llm.repair patch mode requires target or output_file",
                )
            })?;
        let output_path = ctx.render_inline(node, output_path)?;
        let target = ctx.run.fs.resolve_write(&output_path).step_err(&node.id)?;
        // Re-read the source and bind the apply to the shown base: drift
        // between prompt and apply fails closed instead of patching a
        // file the model never saw. The re-read, apply, and commit hold
        // the process-wide per-file exclusion over source and target, so
        // an in-process writer racing this node can only surface as an
        // explicit base mismatch, never as a silent overwrite.
        let source_target = ctx.run.fs.resolve_read(&snapshot.path).step_err(&node.id)?;
        let _guard =
            qcg_engine::lock_patch_paths(&[source_target.as_path(), target.as_path()]).await;
        let current = read_repair_target_text(ctx, node, &snapshot.path, &source_target)?;
        let outcome =
            qcg_fs::apply_anchored_patch(&current, Some(&snapshot.base_sha256), &edits, limits)
                .map_err(|error| StepError::failed(&node.id, repair_patch_error(&error)))?;

        ctx.run
            .fs
            .write_file_atomic(&target, outcome.new_text.as_bytes())
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))?;
        Ok(StepOutcome::Success {
            output: Some(json!({
                "text": outcome.new_text,
                "applied": outcome.applied,
                "base_sha256": outcome.new_base_sha256,
                "file": output_path,
            })),
            files: vec![target],
        })
    }
}

/// Formats an anchored patch failure for the single-shot repair path.
/// Stale anchors carry remaps so the cycle-level retry can show what
/// moved; every other rejection names its cause.
fn repair_patch_error(error: &qcg_fs::AnchoredPatchError) -> String {
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
                "repair patch anchor `{anchor}` is stale (base `{current_base}`); remaps: {}",
                hints.join(", ")
            )
        }
        other => format!("repair patch rejected: {other}"),
    }
}

/// Refuses a self-repair write-back whose snapshot went stale.
/// Pure decision over bytes so the failure path stays testable without
/// a run context.
fn repair_base_allowed(shown_base: &str, current_text: &str) -> bool {
    qcg_fs::base_sha256(current_text) == shown_base
}

fn verify_repair_base(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    target: &camino::Utf8Path,
    snapshot: &crate::prompting::RepairSourceSnapshot,
) -> Result<(), StepError> {
    let current = read_repair_target_text(ctx, node, &snapshot.path, target)?;
    if !repair_base_allowed(&snapshot.base_sha256, &current) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "repair target `{}` changed since the shown snapshot; refusing to overwrite",
                snapshot.path
            ),
        ));
    }
    Ok(())
}

/// Bounded handle-relative read shared by both repair modes.
/// Policy bounds come from `file_input_limit_bytes`; absence means
/// unbounded, matching the surrounding read paths.
fn read_repair_target_text(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    label: &str,
    target: &camino::Utf8Path,
) -> Result<String, StepError> {
    use std::io::Read as _;
    let file = ctx
        .run
        .fs
        .open_read_resolved(target)
        .map_err(|error| StepError::from_gateway(&node.id, error))?;
    let limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
    let cap = limit.map_or(u64::MAX, |value| value.saturating_add(1) as u64);
    let mut bytes = Vec::new();
    file.take(cap)
        .read_to_end(&mut bytes)
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    if let Some(value) = limit
        && bytes.len() > value
    {
        return Err(StepError::failed(
            &node.id,
            format!("repair target `{label}` exceeds {value} bytes"),
        ));
    }
    String::from_utf8(bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("repair target `{label}` is not valid UTF-8: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        RepairMode, normalize_repair_path, repair_base_allowed, repair_mode, repair_patch_error,
    };
    use qcg_contract::NodeDef;
    use serde_json::json;

    fn repair_node(params: serde_json::Value) -> NodeDef {
        serde_json::from_value(json!({
            "id": "repair",
            "type": "llm.repair",
            "params": params,
        }))
        .expect("repair test node should deserialize")
    }

    #[test]
    fn missing_mode_defaults_to_text() {
        let node = repair_node(json!({"prompt": "p"}));
        assert_eq!(
            repair_mode(&node).expect("default mode should resolve"),
            RepairMode::Text
        );
    }

    #[test]
    fn patch_mode_resolves_explicitly() {
        let node = repair_node(json!({"prompt": "p", "mode": "patch"}));
        assert_eq!(
            repair_mode(&node).expect("patch mode should resolve"),
            RepairMode::Patch
        );
    }

    #[test]
    fn unknown_mode_fails_closed() {
        let node = repair_node(json!({"prompt": "p", "mode": "rewrite"}));
        let error = repair_mode(&node).expect_err("unknown mode must fail");
        assert!(
            error.to_string().contains("must be text or patch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unchanged_snapshot_allows_self_repair_write_back() {
        let text = "{\"name\": true}";
        let base = qcg_fs::base_sha256(text);
        assert!(repair_base_allowed(&base, text));
    }

    #[test]
    fn changed_snapshot_refuses_self_repair_write_back() {
        let base = qcg_fs::base_sha256("{\"name\": true}");
        assert!(!repair_base_allowed(&base, "{\"name\": false}"));
    }

    #[test]
    fn repair_path_spelling_does_not_pick_the_semantic() {
        assert_eq!(normalize_repair_path("./notes.txt"), "notes.txt");
        assert_eq!(normalize_repair_path("././notes.txt"), "notes.txt");
        assert_eq!(normalize_repair_path("notes.txt"), "notes.txt");
        assert_eq!(normalize_repair_path("other.txt"), "other.txt");
        assert_ne!(
            normalize_repair_path("./notes.txt"),
            normalize_repair_path("other.txt")
        );
    }

    #[test]
    fn stale_patch_error_reports_remaps() {
        let error = qcg_fs::AnchoredPatchError::AnchorStale {
            anchor: "2:deadbeef".into(),
            remaps: vec![qcg_fs::AnchorRemap {
                stale_anchor: "2:deadbeef".into(),
                current_anchor: "2:015cadfe".into(),
            }],
            current_base: "abc".into(),
        };
        let message = repair_patch_error(&error);
        assert!(
            message.contains("2:deadbeef -> 2:015cadfe"),
            "unexpected message: {message}"
        );
    }
}
