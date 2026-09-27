use super::support::*;
use crate::*;

#[test]
fn explicit_providers_path_is_loaded_without_fallback() {
    let root = temp_run_dir("explicit-providers");
    let _ = std::fs::remove_dir_all(&root);
    let providers_path = root.join("providers.toml");
    let provider_id = format!("explicit-{}", std::process::id());
    std::fs::create_dir_all(&root).expect("provider directory should be created");
    std::fs::write(
        &providers_path,
        format!(
            r#"
[[provider]]
id = "{provider_id}"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#
        ),
    )
    .expect("providers registry should be written");

    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.join("generators")],
        root.join("runs"),
        Some(providers_path.clone()),
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should load the explicit providers registry");
    assert!(
        service
            .inner
            .llm_runtime
            .provider
            .capabilities_for(&provider_id)
            .is_some(),
        "the explicitly selected provider should be registered"
    );

    let missing = root.join("missing.toml");
    let error = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.join("generators")],
        root.join("other-runs"),
        Some(missing.clone()),
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect_err("an explicit missing path must not fall back to another registry");
    assert!(error.to_string().contains(missing.as_str()));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn generator_roots_merge_with_first_root_winning() {
    let primary = temp_run_dir("roots-primary");
    let secondary = temp_run_dir("roots-secondary");
    let runs = temp_run_dir("roots-runs");
    let _ = std::fs::remove_dir_all(&primary);
    let _ = std::fs::remove_dir_all(&secondary);
    let _ = std::fs::remove_dir_all(&runs);
    write_generator_package(&primary, "shared");
    write_generator_package(&primary, "only-primary");
    // The secondary copy of `shared` must never shadow the primary one.
    write_generator_package(&secondary, "shared");
    write_generator_package(&secondary, "bundled-demo");

    let service = test_service(vec![primary.clone(), secondary.clone()], runs.clone());

    let mut listed: Vec<String> = service
        .list_generators()
        .await
        .expect("generators should be listed")
        .into_iter()
        .map(|generator| generator.id)
        .collect();
    listed.sort();
    assert_eq!(listed, vec!["bundled-demo", "only-primary", "shared"]);

    let shared = service
        .load_generator("shared")
        .expect("primary should win");
    assert!(
        shared.root.starts_with(&primary),
        "duplicate id must resolve from the first root, got {}",
        shared.root
    );

    let fallback = service
        .load_generator("bundled-demo")
        .expect("secondary root should resolve");
    assert_eq!(fallback.manifest.generator.id, "bundled-demo");
    assert!(service.load_generator("absent").is_err());

    for dir in [primary, secondary, runs] {
        let _ = std::fs::remove_dir_all(&dir);
    }
}
