use super::*;
use crate::common::{json_test_helpers::Harness, test_helpers::package_node};
use engine::{ContinuationReadCoverage, ContinuationSafety, StepRegistry};

fn node() -> NodeDef {
    package_node(
        "schema",
        "check.schema",
        "source = \"data//value.json\"\nschema = \"data/schema.json\"",
    )
}

fn fixture(name: &str) -> Harness {
    let h = Harness::new(name);
    std::fs::create_dir(h.run.workspace.join("data")).unwrap();
    std::fs::create_dir(h.run.contract.root.join("data")).unwrap();
    std::fs::write(h.run.workspace.join("data/value.json"), b"{}").unwrap();
    std::fs::write(h.run.contract.root.join("data/schema.json"), b"{}").unwrap();
    h
}

#[tokio::test]
async fn exact_source_and_schema_bytes_change_independently() {
    let mut h = fixture("schema-capture-bytes");
    let node = node();
    let owner = ReadOwner::Node(node.id.clone());
    let mut captures = Vec::new();
    for (source, schema, failed) in [
        (b"{}".as_slice(), b"{}".as_slice(), false),
        (b"{ }", b"{}", false),
        (b"{ }", b"{ }", false),
        (b"{ }", br#"{"type":"string"}"#, true),
    ] {
        std::fs::write(h.run.workspace.join("data/value.json"), source).unwrap();
        std::fs::write(h.run.contract.root.join("data/schema.json"), schema).unwrap();
        let mut ctx = h.context();
        let mut vars = ctx.vars.clone();
        ctx.vars = &mut vars;
        let outcome = CheckSchemaStep.execute(&mut ctx, &node).await.unwrap();
        assert_eq!(matches!(outcome, StepOutcome::CheckFailed { .. }), failed);
        let trace = ctx.vars.read_trace();
        assert!(trace.complete_for(&owner));
        assert_eq!(trace.reads.len(), 2);
        for (kind, path, bytes) in [
            (ReadKind::WorkspaceFile, "value.json", source),
            (ReadKind::PackageFile, "schema.json", schema),
        ] {
            let read = trace.reads.iter().find(|read| read.kind == kind).unwrap();
            assert_eq!(read.owner, owner);
            assert_eq!(read.path, ["data", path]);
            assert_eq!(
                read.identity,
                ReadIdentity::Digest(Sha256::digest(bytes).into())
            );
        }
        assert!(ctx.vars.clone().read_trace().reads.is_empty());
        let public = serde_json::to_string(ctx.vars).unwrap();
        assert!(!public.contains("schema.json") && !public.contains("value.json"));
        captures.push(trace);
    }
    for (a, b, changed) in [
        (0, 1, ReadKind::WorkspaceFile),
        (1, 2, ReadKind::PackageFile),
    ] {
        for kind in [ReadKind::WorkspaceFile, ReadKind::PackageFile] {
            let identity = |i: usize| {
                &captures[i]
                    .reads
                    .iter()
                    .find(|r| r.kind == kind)
                    .unwrap()
                    .identity
            };
            assert_eq!(identity(a) != identity(b), kind == changed);
        }
    }
    let mut registry = StepRegistry::new();
    registry.register(CheckSchemaStep);
    assert_eq!(
        registry.continuation_safety(&node),
        ContinuationSafety::ReadOnly
    );
    #[cfg(unix)]
    assert_eq!(
        registry.continuation_read_coverage(&node),
        ContinuationReadCoverage::Complete
    );
    #[cfg(not(unix))]
    assert_eq!(
        registry.continuation_read_coverage(&node),
        ContinuationReadCoverage::Unsupported
    );
    assert_eq!(CheckSchemaStep.continuation_semantics_version(), Some("1"));
}

#[tokio::test]
async fn missing_over_limit_invalid_or_unretainable_inputs_fail_closed() {
    for mode in [
        "source_missing",
        "schema_missing",
        "source_limit",
        "schema_limit",
        "source_json",
        "schema_json",
        "utf8",
        "overflow",
    ] {
        let mut h = fixture(&format!("schema-capture-{mode}"));
        let source = h.run.workspace.join("data/value.json");
        let schema = h.run.contract.root.join("data/schema.json");
        match mode {
            "source_missing" => std::fs::remove_file(source).unwrap(),
            "schema_missing" => std::fs::remove_file(schema).unwrap(),
            "source_limit" => h.run.contract.manifest.runtime.file_input_limit_bytes = Some(1),
            "schema_limit" => {
                std::fs::write(schema, vec![b' '; MAX_JSON_SCHEMA_BYTES + 1]).unwrap()
            }
            "source_json" => std::fs::write(source, b"not json").unwrap(),
            "schema_json" => std::fs::write(schema, b"not json").unwrap(),
            "utf8" => std::fs::write(source, b"\xff").unwrap(),
            "overflow" => (),
            _ => unreachable!(),
        }
        let mut ctx = h.context();
        if mode == "overflow" {
            ctx.vars.read_recorder().set_byte_limit(0);
        }
        let result = CheckSchemaStep.execute(&mut ctx, &node()).await;
        if mode == "overflow" {
            assert!(matches!(result.unwrap(), StepOutcome::Success { .. }));
            assert!(ctx.vars.read_trace().overflow);
        } else {
            assert!(result.is_err(), "{mode}");
        }
        assert!(
            !ctx.vars
                .read_trace()
                .complete_for(&ReadOwner::Node(node().id)),
            "{mode}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn aliases_preserve_execution_but_never_claim_complete_evidence() {
    for schema_alias in [false, true] {
        let mut h = fixture(&format!("schema-capture-alias-{schema_alias}"));
        let path = if schema_alias {
            h.run.contract.root.join("data/schema.json")
        } else {
            h.run.workspace.join("data/value.json")
        };
        let target = path.with_file_name("target.json");
        std::fs::write(&target, b"{}").unwrap();
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let mut ctx = h.context();
        let result = CheckSchemaStep.execute(&mut ctx, &node()).await;
        // Existing resolution permits aliases contained in the root. Both
        // opened-handle readers must still refuse a swapped terminal symlink.
        assert!(matches!(result.unwrap(), StepOutcome::Success { .. }));
        if !schema_alias {
            assert!(captured_transform_text(&ctx.run.fs, &path, Some(1024), None).is_err());
        }
        assert!(
            !ctx.vars
                .read_trace()
                .complete_for(&ReadOwner::Node(node().id))
        );
    }
}
