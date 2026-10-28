use crate::catalog_step::LlmCatalogStep;
use engine::{StepContext, StepExecutor, StepOutcome};
use std::collections::BTreeMap;
use std::sync::Arc;

#[tokio::test]
async fn model_tier_warning_is_emitted_in_catalog_model_output() {
    let root = std::env::temp_dir().join(format!(
        "llm-catalog-warning-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let source = root.join("models.json");
    std::fs::create_dir_all(&root).expect("test directory");
    std::fs::write(
            &source,
            r#"{"openai":{"models":{"tiered-model":{"cost":{"input":1.0,"output":2.0,"tiers":[{"tier":{"type":"context"},"input":3.0}]}}}}}"#,
        )
        .expect("catalog fixture");
    let registry = format!(
        "[[provider]]\nid = \"openai\"\napi = \"chat_completions\"\nbase_url = \"https://example.test/v1\"\n\n[catalog]\ncache = {:?}\n\n[[catalog.source]]\nkind = \"models_dev\"\nfile = {:?}\n",
        root.join("cache.json").to_string_lossy(),
        source.to_string_lossy()
    );
    let runtime = llm::LlmRouter::parse_text(&registry)
        .expect("provider registry")
        .into_runtime();
    runtime
        .catalog
        .refresh()
        .await
        .expect("external fixture should load");

    let generator = root.join("generator");
    let workspace = root.join("workspace");
    let meta = root.join("meta");
    for directory in [&generator, &workspace, &meta] {
        std::fs::create_dir_all(directory).expect("test directory");
    }
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            "[generator]\nid = \"catalog-test\"\nname = \"Catalog Test\"\nversion = \"0.1.0\"\n\n[[flow]]\nid = \"catalog\"\ntype = \"llm.catalog\"\n\n[flow.params]\nselect = \"models\"\nprovider = \"openai\"\n",
        )
        .expect("manifest");
    let contract = contract::Contract::load(
        camino::Utf8PathBuf::from_path_buf(generator).expect("UTF-8 path"),
    )
    .expect("contract");
    let node = contract.manifest.flow[0].clone();
    let run_id = format!("catalog-warning-{}", std::process::id());
    let run = engine::RunContext::for_tests(
        run_id.clone(),
        camino::Utf8PathBuf::from_path_buf(workspace).expect("UTF-8 path"),
        camino::Utf8PathBuf::from_path_buf(meta).expect("UTF-8 path"),
        contract,
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("run context");
    let journal = engine::JournalWriter::create(
        &camino::Utf8PathBuf::from_path_buf(root.join("journal.jsonl")).expect("UTF-8 path"),
        &run_id,
        false,
        None,
    )
    .expect("journal");
    let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
    let mut ctx = StepContext {
        run: &run,
        journal: &journal,
        vars: &mut vars,
        llm: None,
    };
    let step = LlmCatalogStep {
        runtime: Arc::new(runtime),
    };
    let outcome = step
        .execute(&mut ctx, &node)
        .await
        .expect("catalog step succeeds");
    let output = match outcome {
        StepOutcome::Success {
            output: Some(output),
            ..
        } => output,
        other => panic!("unexpected catalog outcome: {other:?}"),
    };
    assert!(output["catalog"]["error"].is_null());
    let warning = output["catalog"]["models"][0]["warning"]
        .as_str()
        .expect("model warning");
    assert!(
        warning.contains("tier schedule could not be used"),
        "{warning}"
    );
    assert_eq!(output["entries"][0]["metadata"]["warning"], warning);
    let _ = std::fs::remove_dir_all(root);
}
