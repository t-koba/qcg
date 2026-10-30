use std::process::Command;

#[test]
fn models_exposes_effective_tiers_in_human_and_api_json_output() {
    let root = std::env::temp_dir().join(format!("qcg-catalog-cli-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let providers = root.join("providers.toml");
    std::fs::write(
        &providers,
        r#"
[[provider]]
id = "catalog-test"
api = "chat_completions"
base_url = "https://example.test/v1"
chat_token_limit_field = "max_completion_tokens"
[[provider.models]]
id = "tiered"
input_cost_per_million_usd = 3.0
output_cost_per_million_usd = 4.0
[[provider.models.cost_tiers]]
threshold_tokens = 200000
input_cost_per_million_usd = 6.0
cache_read_input_cost_per_million_usd = 0.0
[[provider.models.cost_tiers]]
threshold_tokens = 400000
input_cost_per_million_usd = 9.0
output_cost_per_million_usd = 8.0
[[provider.models]]
id = "flat"
[[provider.models]]
id = "unresolved"
[[provider.models.cost_tiers]]
threshold_tokens = 100
input_cost_per_million_usd = 1.0
"#,
    )
    .unwrap();
    let run = |json: bool| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
        command
            .env_clear()
            .current_dir(&root)
            .env("HOME", &root)
            .arg("--providers")
            .arg(&providers)
            .arg("models");
        if json {
            command.arg("--json");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    let human = run(false);
    assert!(
        human
            .lines()
            .next()
            .unwrap()
            .ends_with("cost_tiers(input/read/create/output USD/1M)")
    );
    let row = |id: &str| {
        human
            .lines()
            .find(|line| line.starts_with(&format!("catalog-test\t{id}\t")))
            .unwrap()
    };
    assert!(row("tiered").ends_with(">=200000:6/0/3/4;>=400000:9/3/3/8"));
    assert!(row("flat").ends_with("\t-"));
    assert!(row("unresolved").ends_with(">=100:1/-/-/-"));

    // --json uses the public API DTO, including nulls, zeros, and empty schedules.
    let catalog: api::LlmCatalogResponse = serde_json::from_str(&run(true)).unwrap();
    let provider = catalog
        .providers
        .iter()
        .find(|provider| provider.id == "catalog-test")
        .unwrap();
    let model = |id: &str| provider.models.iter().find(|model| model.id == id).unwrap();
    let tiered = model("tiered");
    assert_eq!(tiered.input_cost_per_million_usd, Some(3.0));
    assert_eq!(tiered.output_cost_per_million_usd, Some(4.0));
    assert_eq!(tiered.cache_creation_input_cost_per_million_usd, None);
    let tiers = serde_json::to_value(&tiered.cost_tiers).unwrap();
    assert_eq!(
        tiers,
        serde_json::json!([
            {"threshold_tokens": 200000, "input_cost_per_million_usd": 6.0,
             "cache_read_input_cost_per_million_usd": 0.0, "cache_creation_input_cost_per_million_usd": 3.0,
             "output_cost_per_million_usd": 4.0},
            {"threshold_tokens": 400000, "input_cost_per_million_usd": 9.0,
             "cache_read_input_cost_per_million_usd": 3.0, "cache_creation_input_cost_per_million_usd": 3.0,
             "output_cost_per_million_usd": 8.0}
        ])
    );
    assert!(model("flat").cost_tiers.is_empty());
    let unresolved = serde_json::to_value(model("unresolved")).unwrap();
    assert!(unresolved["cost_tiers"][0]["cache_creation_input_cost_per_million_usd"].is_null());
    assert!(unresolved["cost_tiers"][0]["output_cost_per_million_usd"].is_null());
    std::fs::remove_dir_all(root).unwrap();
}
