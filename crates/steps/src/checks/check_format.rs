use crate::common::{read_opened_bounded, require};
use async_trait::async_trait;
use contract::{Contract, NodeDef, ReadIdentity, ReadKind, ReadOwner};
use engine::{ContinuationSafety, StepContext, StepError, StepExecutor, StepOutcome};
use model::Finding;
use policy::{params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(crate) struct CheckFormatStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckFormatParams {
    source: String,
    content: String,
}

#[async_trait]
impl StepExecutor for CheckFormatStep {
    engine::step_type_id!("check.format");

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "content"],
            json!({
                "source": string_schema(),
                "content": { "type": "string", "enum": ["json", "toml"] },
            }),
        ))
    }

    engine::parallel_step_traits!();

    fn continuation_safety(&self, _node: &NodeDef) -> ContinuationSafety {
        ContinuationSafety::ReadOnly
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_format_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_format_params(node)?;
        let reads = ctx
            .vars
            .read_recorder()
            .for_owner(ReadOwner::Node(node.id.clone()));
        let source = ctx
            .render_inline(node, &params.source)
            .inspect_err(|_| reads.unsupported())?;
        let text = (|| {
            let source_path = ctx
                .run
                .fs
                .resolve_read(&source)
                .map_err(|error| format!("source path is not in workspace: {error}"))?;
            let file = ctx
                .run
                .fs
                .open_read_resolved(&source_path)
                .map_err(|error| error.to_string())?;
            // Evidence is optional. Do not re-open or re-read the path: the
            // digest covers precisely the buffer passed to the parser below.
            let regular = file.metadata().is_ok_and(|meta| meta.is_file());
            let key = ctx.run.fs.workspace_relative_key(&source_path).ok();
            let bytes = read_opened_bounded(
                file,
                ctx.run.contract.manifest.runtime.file_input_limit_bytes,
                "transform input",
            )?;
            if let Some(key) = key.filter(|_| regular) {
                if reads.is_recording() {
                    reads.record(
                        &key.split('/').map(str::to_owned).collect::<Vec<_>>(),
                        ReadKind::WorkspaceFile,
                        ReadIdentity::Digest(Sha256::digest(&bytes).into()),
                    );
                }
            } else {
                reads.unsupported();
            }
            String::from_utf8(bytes)
                .map_err(|error| format!("transform input is not valid UTF-8: {error}"))
        })()
        .map_err(|error| {
            reads.unsupported();
            StepError::failed(&node.id, error)
        })?;
        let format = params.content.as_str();
        let result = match format {
            "json" => serde_json::from_str::<Value>(&text)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            "toml" => toml::from_str::<toml::Value>(&text)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            _ => {
                // Guarded by `check_format_params` above; a future caller
                // bypassing validation must fail the step, never panic.
                return Err(StepError::failed(
                    &node.id,
                    format!("unsupported check.format content `{format}`"),
                ));
            }
        };
        match result {
            Ok(()) => Ok(StepOutcome::success(Some(
                json!({ "status": "pass", "source": source, "format": format }),
            ))),
            Err(error) => Ok(StepOutcome::check_failed(vec![Finding::error(
                format!("invalid {format}: {error}"),
                Some(source),
                None,
            )])),
        }
    }
}

fn check_format_params(node: &NodeDef) -> Result<CheckFormatParams, StepError> {
    let params: CheckFormatParams = engine::step_params(node, "check.format")?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.content), "content")?;
    if !matches!(params.content.as_str(), "json" | "toml") {
        return Err(StepError::failed(
            &node.id,
            "content must be one of: json, toml",
        ));
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{json_test_helpers::Harness, test_helpers::package_node};

    #[test]
    fn continuation_safety_is_read_only() {
        let node: NodeDef = serde_json::from_value(serde_json::json!({
            "id": "format",
            "type": "check.format"
        }))
        .unwrap();
        let mut registry = engine::StepRegistry::new();
        registry.register(CheckFormatStep);
        assert_eq!(
            registry.continuation_safety(&node),
            ContinuationSafety::ReadOnly
        );
    }

    fn node(source: &str) -> NodeDef {
        package_node(
            "loop[0]/format",
            "check.format",
            &format!("source = {source:?}\ncontent = \"json\""),
        )
    }

    #[tokio::test]
    async fn file_identity_is_of_consumed_bytes_not_path_or_parsed_value() {
        let mut h = Harness::new("format-file-identity");
        std::fs::create_dir(h.run.workspace.join("data")).unwrap();
        let node = node("data//source.json");
        let owner = ReadOwner::Node(node.id.clone());
        let mut identities = Vec::new();
        for bytes in [b"{\"x\":1}".as_slice(), b"{ \"x\": 1 }", b"not json"] {
            std::fs::write(h.run.workspace.join("data/source.json"), bytes).unwrap();
            let mut ctx = h.context();
            // A fresh bag is a fork: it inherits policy but no prior evidence.
            let mut vars = ctx.vars.clone();
            assert!(vars.read_trace().reads.is_empty());
            ctx.vars = &mut vars;
            let result = CheckFormatStep.execute(&mut ctx, &node).await.unwrap();
            assert_eq!(
                matches!(result, StepOutcome::CheckFailed { .. }),
                bytes == b"not json"
            );
            let trace = ctx.vars.read_trace();
            assert!(trace.complete_for(&owner));
            let files: Vec<_> = trace
                .reads
                .iter()
                .filter(|read| read.kind == ReadKind::WorkspaceFile)
                .collect();
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].owner, owner);
            assert_eq!(files[0].path, ["data", "source.json"]);
            assert_eq!(
                files[0].identity,
                ReadIdentity::Digest(Sha256::digest(bytes).into())
            );
            identities.push(files[0].identity.clone());
            // Cloning after capture must not carry this file evidence into a
            // fork or rebind it to a new run/owner.
            assert!(ctx.vars.clone().read_trace().reads.is_empty());
            let public = serde_json::to_string(ctx.vars).unwrap();
            assert!(!public.contains("workspace_file") && !public.contains("source.json"));
        }
        assert_ne!(identities[0], identities[1]);
        assert_ne!(identities[1], identities[2]);
    }

    #[tokio::test]
    async fn missing_or_unretainable_evidence_does_not_change_execution() {
        for mode in ["missing", "input_limit", "invalid_utf8", "trace_limit"] {
            let mut h = Harness::new(&format!("format-evidence-{mode}"));
            let path = h.run.workspace.join("source.json");
            if mode != "missing" {
                std::fs::write(
                    &path,
                    if mode == "invalid_utf8" {
                        b"\xff".as_slice()
                    } else {
                        b"{}"
                    },
                )
                .unwrap();
            }
            if mode == "input_limit" {
                h.run.contract.manifest.runtime.file_input_limit_bytes = Some(1);
            }
            let mut ctx = h.context();
            if mode == "trace_limit" {
                ctx.vars.read_recorder().set_byte_limit(0);
            }
            let node = node("source.json");
            let result = CheckFormatStep.execute(&mut ctx, &node).await;
            if mode == "trace_limit" {
                assert!(matches!(result.unwrap(), StepOutcome::Success { .. }));
            } else {
                assert!(result.is_err(), "{mode}");
            }
            assert!(
                !ctx.vars
                    .read_trace()
                    .complete_for(&ReadOwner::Node(node.id))
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonportable_file_identity_is_incomplete_not_a_step_failure() {
        let mut h = Harness::new("format-unsafe-evidence");
        std::fs::write(h.run.workspace.join("source?.json"), b"{}").unwrap();
        let mut ctx = h.context();
        let node = node("source?.json");
        assert!(matches!(
            CheckFormatStep.execute(&mut ctx, &node).await.unwrap(),
            StepOutcome::Success { .. }
        ));
        assert!(
            !ctx.vars
                .read_trace()
                .complete_for(&ReadOwner::Node(node.id))
        );
    }
}
