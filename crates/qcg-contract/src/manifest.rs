pub mod assets;
pub mod contract;
pub mod flow;
pub mod llm;
pub mod metadata;
pub mod nodes;
pub mod outputs;
pub mod resources;
pub mod tools;
pub mod validate;
pub use contract::*;
#[cfg(test)]
pub(crate) use flow::*;
pub use llm::*;
pub use nodes::*;
pub use outputs::*;
pub use resources::*;
pub use tools::*;
pub use validate::*;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentFailureAction, AgentFailureCode, AssetSpec, FieldType, GeneratorMeta, InputField,
        InputSpec, InputStage, RecoverableAgentFailureCode,
    };
    use camino::Utf8PathBuf;
    use qcg_policy::{
        MAX_JSON_SCHEMA_DEPTH, MAX_JSON_SCHEMA_STRING_BYTES, validate_bounded_json_schema,
    };
    use qcg_types::{ArtifactPreview, FileValue};
    use serde::Deserialize;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::fs;
    fn manifest_with_llm_settings(settings: &str) -> Manifest {
        let max_tokens = if settings
            .lines()
            .any(|line| line.trim_start().starts_with("max_tokens"))
        {
            ""
        } else {
            "max_tokens = 2048"
        };
        toml::from_str(&format!(
            r#"
[generator]
id = "llm-settings"
name = "LLM Settings"
version = "0.1.0"
qcg_version = "^0.1"
[llm]
{max_tokens}
{settings}
"#
        ))
        .expect("manifest should parse")
    }
    fn manifest_with_field(field: InputField) -> Manifest {
        Manifest {
            generator: GeneratorMeta {
                id: "test".into(),
                name: "Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            llm: None,
            inputs: InputSpec {
                stages: vec![InputStage {
                    id: "basic".into(),
                    when: None,
                    fields: vec![field],
                }],
            },
            resources: BTreeMap::new(),
            tools: BTreeMap::new(),
            permissions: Permissions::default(),
            secrets: BTreeMap::new(),
            runtime: RuntimeLimits::default(),
            budget: RunBudget::default(),
            flow: vec![],
            parallel: vec![],
            blocks: BTreeMap::new(),
            outputs: OutputSpec::default(),
            failure: FailurePolicy::default(),
            journal: JournalPolicy::default(),
            assets: AssetSpec::default(),
            dependencies: BTreeMap::new(),
        }
    }
    fn input_field_defaults() -> InputField {
        InputField {
            id: String::new(),
            label: None,
            label_i18n: BTreeMap::new(),
            description: None,
            description_i18n: BTreeMap::new(),
            placeholder: None,
            placeholder_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: Vec::new(),
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            schema: None,
            ui: Default::default(),
        }
    }
    fn retry_node(id: &str, retry: Option<RetryPolicy>) -> NodeDef {
        NodeDef {
            id: id.into(),
            kind: StepType::from("write"),
            needs: vec![],
            when: None,
            on_deps: OnDeps::default(),
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry,
            params: toml::Table::new(),
        }
    }
    fn flow_manifest(nodes: Vec<NodeDef>) -> Manifest {
        Manifest {
            generator: GeneratorMeta {
                id: "test".into(),
                name: "Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            llm: None,
            inputs: InputSpec::default(),
            resources: BTreeMap::new(),
            tools: BTreeMap::new(),
            permissions: Permissions::default(),
            secrets: BTreeMap::new(),
            runtime: RuntimeLimits::default(),
            budget: RunBudget::default(),
            flow: nodes,
            parallel: Vec::new(),
            blocks: BTreeMap::new(),
            outputs: OutputSpec::default(),
            failure: FailurePolicy::default(),
            journal: JournalPolicy::default(),
            assets: AssetSpec::default(),
            dependencies: BTreeMap::new(),
        }
    }
    #[test]
    fn retry_policy_bounds_are_enforced() {
        let valid = flow_manifest(vec![retry_node(
            "flaky",
            Some(RetryPolicy {
                max_attempts: 3,
                backoff_ms: 100,
                timeout_secs: Some(30),
                on_indeterminate: RetryOnIndeterminate::Fail,
            }),
        )]);
        FlowNodeRule
            .validate(&valid)
            .expect("bounded retry policy should pass");
        let unset = flow_manifest(vec![retry_node("plain", None)]);
        FlowNodeRule
            .validate(&unset)
            .expect("omitted retry should pass");
        for (name, retry) in [
            (
                "zero attempts",
                RetryPolicy {
                    max_attempts: 0,
                    backoff_ms: 0,
                    timeout_secs: None,
                    on_indeterminate: RetryOnIndeterminate::Fail,
                },
            ),
            (
                "too many attempts",
                RetryPolicy {
                    max_attempts: 17,
                    backoff_ms: 0,
                    timeout_secs: None,
                    on_indeterminate: RetryOnIndeterminate::Fail,
                },
            ),
            (
                "excessive backoff",
                RetryPolicy {
                    max_attempts: 2,
                    backoff_ms: 60_001,
                    timeout_secs: None,
                    on_indeterminate: RetryOnIndeterminate::Fail,
                },
            ),
            (
                "zero timeout",
                RetryPolicy {
                    max_attempts: 2,
                    backoff_ms: 0,
                    timeout_secs: Some(0),
                    on_indeterminate: RetryOnIndeterminate::Fail,
                },
            ),
        ] {
            let error = FlowNodeRule
                .validate(&flow_manifest(vec![retry_node("flaky", Some(retry))]))
                .expect_err("out-of-bounds retry policy must fail");
            assert!(
                error.to_string().contains("retry."),
                "{name} should mention retry: {error}"
            );
        }
    }
    #[test]
    fn agent_failure_policy_returns_recoverable_errors_and_propagates_run_boundaries() {
        let policy = AgentFailurePolicy::default();
        assert_eq!(
            policy.action(AgentFailureCode::TokenBudgetExceeded),
            AgentFailureAction::ReturnError
        );
        assert_eq!(
            policy.action(AgentFailureCode::ProviderFailed),
            AgentFailureAction::ReturnError
        );
        assert_eq!(
            policy.action(AgentFailureCode::RunBudgetExceeded),
            AgentFailureAction::Fail
        );
        assert_eq!(
            policy.action(AgentFailureCode::Cancelled),
            AgentFailureAction::Fail
        );
        let policy = AgentFailurePolicy {
            default: AgentFailureAction::ReturnError,
            by_code: BTreeMap::from([(
                RecoverableAgentFailureCode::ProviderFailed,
                AgentFailureAction::Fail,
            )]),
        };
        assert_eq!(
            policy.action(AgentFailureCode::ProviderFailed),
            AgentFailureAction::Fail
        );
        assert_eq!(
            policy.action(AgentFailureCode::ValidationFailed),
            AgentFailureAction::ReturnError
        );
        let error = toml::from_str::<AgentFailurePolicy>(
            "default = \"return_error\"\n[by_code]\ncancelled = \"return_error\"",
        )
        .expect_err("cancellation must not be exposed as a recoverable policy key");
        assert!(error.to_string().contains("unknown variant"));
    }
    #[test]
    fn runtime_size_limits_default_to_unlimited_and_reject_zero() {
        let defaults = RuntimeLimits::default();
        assert_eq!(defaults.output_file_limit_bytes, None);
        assert_eq!(defaults.output_total_limit_bytes, None);
        assert_eq!(defaults.output_artifact_limit, None);
        assert_eq!(defaults.template_source_limit_bytes, None);
        assert_eq!(defaults.template_context_limit_bytes, None);
        assert_eq!(defaults.template_output_limit_bytes, None);
        assert_eq!(defaults.file_input_limit_bytes, None);
        assert_eq!(defaults.input_total_limit_bytes, None);
        assert_eq!(defaults.template_fuel, 1_000_000);
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            ..input_field_defaults()
        });
        manifest.generator.qcg_version = "^0.1".into();
        manifest
            .validate()
            .expect("unlimited size limits should validate");
        manifest.runtime.template_output_limit_bytes = Some(0);
        let error = manifest
            .validate()
            .expect_err("zero template output limit must be rejected");
        assert!(error.to_string().contains("template_output_limit_bytes"));
        manifest.runtime.template_output_limit_bytes = None;
        manifest.runtime.template_fuel = 0;
        let error = manifest
            .validate()
            .expect_err("zero template fuel must be rejected");
        assert!(error.to_string().contains("template_fuel"));
        manifest.runtime.template_fuel = defaults.template_fuel;
        manifest.runtime.template_source_limit_bytes = Some(0);
        let error = manifest
            .validate()
            .expect_err("zero template source limit must be rejected");
        assert!(error.to_string().contains("template_source_limit_bytes"));
        manifest.runtime.template_source_limit_bytes = None;
        manifest.runtime.template_context_limit_bytes = Some(0);
        let error = manifest
            .validate()
            .expect_err("zero template context limit must be rejected");
        assert!(error.to_string().contains("template_context_limit_bytes"));
        manifest.runtime.template_context_limit_bytes = None;
        manifest.runtime.output_file_limit_bytes = Some(0);
        let error = manifest
            .validate()
            .expect_err("zero output file limit must be rejected");
        assert!(error.to_string().contains("output_file_limit_bytes"));
        manifest.runtime.output_file_limit_bytes = None;
        manifest.runtime.output_total_limit_bytes = Some(0);
        let error = manifest
            .validate()
            .expect_err("zero output total limit must be rejected");
        assert!(error.to_string().contains("output_total_limit_bytes"));
        manifest.runtime.output_total_limit_bytes = None;
        manifest.runtime.output_artifact_limit = Some(0);
        let error = manifest
            .validate()
            .expect_err("zero output artifact limit must be rejected");
        assert!(error.to_string().contains("output_artifact_limit"));
        // Explicit large limits have no mechanistic ceiling.
        manifest.runtime.output_artifact_limit = Some(usize::MAX);
        manifest.runtime.output_total_limit_bytes = Some(usize::MAX);
        manifest
            .validate()
            .expect("explicit large limits must validate");
    }
    #[test]
    fn runtime_and_budget_limits_have_no_hard_ceiling() {
        let base = manifest_with_field(InputField {
            id: "name".into(),
            ..input_field_defaults()
        });
        let mut manifest = base.clone();
        manifest.runtime.command_output_limit_bytes = Some(usize::MAX);
        manifest.runtime.file_count_limit = Some(usize::MAX);
        manifest.runtime.http_redirect_limit = Some(usize::MAX);
        manifest
            .validate()
            .expect("explicit large size limits must validate");
    }
    #[test]
    fn resolve_inputs_applies_defaults_and_patterns() {
        let manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            description: None,
            description_i18n: BTreeMap::new(),
            placeholder: None,
            placeholder_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: true,
            default: Some(Value::String("alpha".into())),
            pattern: Some("^[a-z]+$".into()),
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            schema: None,
            ui: Default::default(),
        });
        let resolved = manifest.resolve_inputs(BTreeMap::new()).unwrap();
        assert_eq!(resolved.get("name"), Some(&Value::String("alpha".into())));
    }
    #[test]
    fn input_schema_validates_defaults_and_resolved_values() {
        let manifest = manifest_with_field(InputField {
            id: "count".into(),
            kind: FieldType::Number,
            required: true,
            schema: Some(serde_json::json!({
                "type": "number",
                "minimum": 2,
                "maximum": 4
            })),
            ..input_field_defaults()
        });
        manifest
            .validate()
            .expect("valid field schema should compile");
        let error = manifest
            .resolve_inputs(BTreeMap::from([("count".into(), serde_json::json!(1))]))
            .expect_err("value outside field schema should fail");
        assert!(error.to_string().contains("JSON Schema"));
        let invalid_default = manifest_with_field(InputField {
            id: "count".into(),
            kind: FieldType::Number,
            default: Some(serde_json::json!(5)),
            schema: Some(serde_json::json!({ "type": "number", "maximum": 4 })),
            ..input_field_defaults()
        });
        invalid_default
            .validate()
            .expect_err("invalid default should fail contract validation");
        let custom = manifest_with_field(InputField {
            id: "coordinates".into(),
            kind: FieldType::Custom("geo.point".into()),
            required: true,
            schema: Some(serde_json::json!({
                "type": "object",
                "required": ["lat", "lon"],
                "properties": {
                    "lat": { "type": "number" },
                    "lon": { "type": "number" }
                }
            })),
            ..input_field_defaults()
        });
        custom
            .resolve_inputs(BTreeMap::from([(
                "coordinates".into(),
                serde_json::json!({ "lat": 35.0, "lon": 139.0 }),
            )]))
            .expect("custom fields should derive their value shape from schema");
        for kind in ["geo.point", "Geo Point"] {
            let manifest = manifest_with_field(InputField {
                id: "coordinates".into(),
                kind: FieldType::Custom(kind.into()),
                required: true,
                schema: None,
                ..input_field_defaults()
            });
            let error = manifest
                .validate()
                .expect_err("custom fields without a valid kind and schema must fail");
            assert!(
                error.to_string().contains("requires schema")
                    || error.to_string().contains("invalid custom field type")
            );
        }
    }
    #[test]
    fn validates_on_exhausted_forms_inside_reusable_blocks() {
        let mut manifest = manifest_with_field(InputField {
            id: "request".into(),
            ..input_field_defaults()
        });
        manifest.blocks.insert(
            "retry".into(),
            vec![NodeDef {
                id: "generate".into(),
                kind: StepType::new("render"),
                needs: Vec::new(),
                when: None,
                on_deps: OnDeps::default(),
                context: Vec::new(),
                output: None,
                artifact: None,
                on_fail: Some(OnFail::Regenerate {
                    max_attempts: 1,
                    on_exhausted: ExhaustedAction::AskUser {
                        title: None,
                        fields: vec![input_field_defaults()],
                    },
                }),
                failure: None,
                retry: None,
                params: toml::Table::new(),
            }],
        );
        let error = manifest
            .validate()
            .expect_err("an empty block form field id should be rejected");
        assert!(error.to_string().contains("on_exhausted input field id"));
    }
    #[test]
    fn llm_reasoning_effort_accepts_every_supported_level() {
        for effort in ["none", "minimal", "low", "medium", "high", "xhigh", "max"] {
            manifest_with_llm_settings(&format!("reasoning_effort = \"{effort}\""))
                .validate()
                .expect("reasoning effort should validate");
        }
    }
    #[test]
    fn llm_reasoning_effort_rejects_sampling_controls() {
        for field in ["temperature = 0.5", "seed = 42"] {
            let manifest =
                manifest_with_llm_settings(&format!("reasoning_effort = \"high\"\n{field}"));
            let error = manifest
                .validate()
                .expect_err("reasoning and sampling controls must not be mixed");
            assert!(error.to_string().contains("must be omitted"));
        }
    }
    #[test]
    fn llm_limits_reject_invalid_values() {
        for settings in [
            "max_tokens = 0",
            "temperature = -0.1",
            "temperature = 2.1",
            "max_context_bytes = 0",
            "max_context_tokens = 0",
            "max_media_bytes = 0",
        ] {
            let error = manifest_with_llm_settings(settings)
                .validate()
                .expect_err("invalid LLM limits must be rejected");
            assert!(error.to_string().contains("[llm]."));
        }
        // Explicit large limits have no mechanistic ceiling.
        for settings in [
            "max_context_bytes = 1073741825",
            "max_context_tokens = 268435457",
            "max_media_bytes = 1073741825",
        ] {
            manifest_with_llm_settings(settings)
                .validate()
                .expect("explicit large LLM limits must validate");
        }
    }
    #[test]
    fn llm_max_tokens_is_required() {
        let mut manifest = manifest_with_llm_settings("");
        manifest.llm.as_mut().expect("LLM config").max_tokens = None;
        let error = manifest
            .validate()
            .expect_err("max_tokens must be explicit");
        assert!(error.to_string().contains("max_tokens is required"));
    }
    #[test]
    fn llm_requires_rejects_unknown_and_duplicate_capabilities() {
        for settings in [
            "requires = [\"unknown\"]",
            "requires = [\"json_schema\", \"json_schema\"]",
        ] {
            let error = manifest_with_llm_settings(settings)
                .validate()
                .expect_err("invalid requires entries must fail");
            assert!(error.to_string().contains("[llm].requires"));
        }
        manifest_with_llm_settings("requires = [\"structured_output_with_tools\"]")
            .validate()
            .expect("structured output with tools is a declared provider capability");
    }
    #[test]
    fn llm_model_names_must_not_be_empty() {
        for model in [
            "model = { provider = \"\", model = \"gpt\" }",
            "model = { provider = \"openai\", model = \"\" }",
        ] {
            let error = manifest_with_llm_settings(model)
                .validate()
                .expect_err("empty model identifiers must fail");
            assert!(error.to_string().contains("must not be empty"));
        }
    }
    #[test]
    fn resolve_inputs_validates_file_values_as_canonical_objects() {
        let manifest = manifest_with_field(InputField {
            id: "attachment".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::File,
            required: true,
            default: None,
            pattern: Some(r"^[a-z]+\.txt$".into()),
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        let file = FileValue::from_text("note.txt", "hello").expect("file should be valid");
        let resolved = manifest
            .resolve_inputs(BTreeMap::from([(
                "attachment".into(),
                serde_json::to_value(file).expect("file should encode"),
            )]))
            .expect("valid file input should resolve");
        assert!(resolved.contains_key("attachment"));
        let error = manifest
            .resolve_inputs(BTreeMap::from([(
                "attachment".into(),
                serde_json::json!({"name": "../note.txt", "text": "hello"}),
            )]))
            .expect_err("unsafe file names must be rejected");
        assert!(error.to_string().contains("invalid file value"));
    }
    #[test]
    fn assets_must_be_declared_safe_package_paths() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.assets.files = vec!["ui/index.html".into(), "ui/app.js".into()];
        manifest
            .validate()
            .expect("declared assets should validate");
        manifest.assets.files[1] = "../app.js".into();
        let error = manifest
            .validate()
            .expect_err("path traversal must be rejected");
        assert!(error.to_string().contains("safe relative path"));
    }
    #[test]
    fn assets_accept_arbitrary_extensions_and_extensionless_files() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.assets.files = vec!["bundle.wasm".into(), "NOTICE".into()];
        manifest
            .validate()
            .expect("declared assets with arbitrary names should validate");
    }
    #[test]
    fn contract_load_allows_unbuilt_asset_directories_but_not_missing_asset_files() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must follow the Unix epoch")
            .as_nanos();
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-contract-unbuilt-assets-{}-{unique}",
            std::process::id()
        )))
        .expect("temporary path must be UTF-8");
        fs::create_dir(&root).expect("temporary contract directory must be created");
        fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "unbuilt-assets"
version = "0.1.0"
qcg_version = "^0.1"
[assets]
dirs = ["ui"]
meta = { entry = "ui/index.html" }
"#,
        )
        .expect("manifest must be written");
        Contract::load(&root).expect("an unbuilt declared asset directory must remain loadable");
        fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "missing-file-asset"
version = "0.1.0"
qcg_version = "^0.1"
[assets]
files = ["ui/index.html"]
"#,
        )
        .expect("manifest must be replaced");
        let error = Contract::load(&root)
            .expect_err("an explicitly declared missing asset file must be rejected");
        assert!(error.to_string().contains("asset file `ui/index.html`"));
        fs::remove_dir_all(&root).expect("temporary contract directory must be removed");
    }
    #[test]
    fn output_artifact_mime_must_be_a_valid_media_type() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.outputs.extras.push(OutputExtraDef {
            glob: "reports/*.json".into(),
            label: "Reports".into(),
            required: false,
            mime: Some("not a media type".into()),
            description: String::new(),
            preview: ArtifactPreview::Auto,
        });
        let error = manifest
            .validate()
            .expect_err("invalid artifact MIME type must be rejected");
        assert!(error.to_string().contains("invalid MIME type"));
    }
    #[test]
    fn resolve_inputs_rejects_missing_required_values() {
        let manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        assert!(manifest.resolve_inputs(BTreeMap::new()).is_err());
    }
    #[test]
    fn resolve_inputs_rejects_short_lists() {
        let manifest = manifest_with_field(InputField {
            id: "items".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::List,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: Some(2),
            item_type: Some(FieldType::String),
            ..input_field_defaults()
        });
        let mut input = BTreeMap::new();
        input.insert(
            "items".into(),
            Value::Array(vec![Value::String("one".into())]),
        );
        assert!(manifest.resolve_inputs(input).is_err());
    }
    #[test]
    fn contract_load_rejects_a_future_qcg_runtime_requirement() {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-contract-version-{}", std::process::id())),
        )
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary contract directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "version-check"
version = "0.1.0"
qcg_version = ">=99"
"#,
        )
        .expect("manifest should be written");
        let error = Contract::load(&root).expect_err("future runtime requirement must fail");
        assert!(error.to_string().contains("requires qcg_version"));
    }
    #[test]
    fn contract_load_rejects_resource_paths_before_loader_dispatch() {
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-contract-resource-path-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        )))
        .expect("temporary path should be UTF-8");
        std::fs::create_dir_all(&root).expect("temporary contract directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "resource-path-check"
version = "0.1.0"
qcg_version = "^0.1"
[resources.escape]
type = "file"
path = "../outside.txt"
"#,
        )
        .expect("manifest should be written");
        let error = Contract::load(&root).expect_err("unsafe resource path must fail at load time");
        assert!(
            error.to_string().contains("resource `escape` path"),
            "{error}"
        );
        assert!(error.to_string().contains("safe relative path"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary contract directory should be removed");
    }
    #[test]
    fn resource_sources_are_exclusive_and_builtin_sources_are_required() {
        let cases = [
            (
                r#"
[resources.ambiguous]
type = "file"
path = "resource.txt"
url = "https://example.test/resource.txt"
"#,
                "requires path and forbids url",
            ),
            (
                r#"
[resources.missing]
type = "file"
"#,
                "type `file` requires path",
            ),
            (
                r#"
[resources.missing]
type = "url"
"#,
                "type `url` requires url",
            ),
            (
                r#"
[resources.missing]
type = "openapi"
"#,
                "type `openapi` requires exactly one of path or url",
            ),
        ];
        for (resource, expected) in cases {
            let manifest: Manifest = toml::from_str(&format!(
                r#"
[generator]
id = "resource-validation"
version = "0.1.0"
qcg_version = "^0.1"
{resource}
"#
            ))
            .expect("manifest should parse");
            let error = manifest
                .validate()
                .expect_err("invalid resource source declaration must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
    #[test]
    fn exec_resource_requires_a_declared_bounded_command() {
        let valid: Manifest = toml::from_str(
            r#"
[generator]
id = "exec-resource"
version = "0.1.0"
qcg_version = "^0.1"
[resources.generated]
type = "exec"
llm_visible = true
[resources.generated.params]
command = ["printf", "hello"]
max_bytes = 1024
[[permissions.commands]]
bin = "printf"
args = ["hello"]
purpose = "load deterministic resource"
isolation = "trusted_host"
"#,
        )
        .expect("manifest should parse");
        valid.validate().expect("exec resource should validate");
        let mut oversized = valid.clone();
        oversized.runtime.command_output_limit_bytes = Some(1024);
        oversized
            .resources
            .get_mut("generated")
            .expect("resource must exist")
            .params
            .insert("max_bytes".into(), Value::from(5 * 1024 * 1024));
        let error = oversized
            .validate()
            .expect_err("resource limit above the command capture limit must fail");
        assert!(error.to_string().contains("command_output_limit_bytes"));
        for (manifest, expected) in [
            (
                r#"
[generator]
id = "exec-resource"
version = "0.1.0"
qcg_version = "^0.1"
[resources.generated]
type = "exec"
[resources.generated.params]
command = ["printf", "hello"]
"#,
                "not declared in permissions.commands",
            ),
            (
                r#"
[generator]
id = "exec-resource"
version = "0.1.0"
qcg_version = "^0.1"
[resources.generated]
type = "exec"
path = "resource.txt"
[resources.generated.params]
command = ["printf", "hello"]
"#,
                "forbids path and url",
            ),
        ] {
            let manifest: Manifest = toml::from_str(manifest).expect("manifest should parse");
            let error = manifest
                .validate()
                .expect_err("invalid exec resource must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
    #[test]
    fn contract_load_rejects_resource_paths_with_wrong_types() {
        let cases = [
            ("file", true, "must be a file"),
            ("dir", false, "must be a directory"),
            ("openapi", true, "must be a file"),
        ];
        for (index, (kind, create_directory, expected)) in cases.into_iter().enumerate() {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
                "qcg-contract-resource-type-{}-{index}",
                std::process::id()
            )))
            .expect("temporary path should be UTF-8");
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).expect("temporary contract directory should be created");
            let resource_path = root.join("resource");
            if create_directory {
                std::fs::create_dir_all(&resource_path)
                    .expect("resource directory should be created");
            } else {
                std::fs::write(&resource_path, "resource")
                    .expect("resource file should be written");
            }
            std::fs::write(
                root.join("qcg.toml"),
                format!(
                    r#"
[generator]
id = "resource-type-validation"
version = "0.1.0"
qcg_version = "^0.1"
[resources.test]
type = "{kind}"
path = "resource"
"#
                ),
            )
            .expect("manifest should be written");
            let error = Contract::load(&root)
                .expect_err("resource path with the wrong type must fail at load time");
            assert!(error.to_string().contains(expected), "{error}");
            std::fs::remove_dir_all(root).expect("temporary contract directory should be removed");
        }
    }
    #[test]
    fn step_type_accepts_namespaced_lowercase_ids() {
        let step_type = StepType::parse("llm.generate").expect("step type should be valid");
        assert_eq!(step_type.as_str(), "llm.generate");
    }
    #[test]
    fn step_type_rejects_uppercase_and_separators() {
        assert!(StepType::parse("Llm.Generate").is_err());
        assert!(StepType::parse("llm-generate").is_err());
        assert!(StepType::parse("").is_err());
    }
    #[test]
    fn resource_kind_accepts_builtins() {
        let resource: ResourceDef =
            toml::from_str("type = \"openapi\"\nurl = \"https://example.test/openapi.json\"")
                .expect("built-in resource kind should deserialize");
        assert_eq!(resource.kind, ResourceKind::Openapi);
    }
    #[test]
    fn node_def_rejects_step_params_outside_params_table() {
        let source = r#"
id = "echo"
type = "demo.echo"
message = "hello"
destination = "result.txt"
"#;
        let error = toml::from_str::<NodeDef>(source)
            .expect_err("step params outside params must be rejected");
        assert!(error.to_string().contains("unknown field"));
    }
    #[test]
    fn node_def_params_json_contains_only_closed_params_table() {
        let source = r#"
id = "write"
type = "write"
context = ["inputs.*"]
[params]
output_file = "result.txt"
content = "hello"
"#;
        let node: NodeDef = toml::from_str(source).expect("node should parse");
        let params = node.params_json();
        assert_eq!(params["output_file"], Value::String("result.txt".into()));
        assert_eq!(params["content"], Value::String("hello".into()));
        assert!(params.get("context").is_none());
        assert_eq!(node.context, vec![ContextRef::Short("inputs.*".into())]);
    }
    #[test]
    fn node_def_parses_closed_resource_context_selector() {
        let source = r#"
id = "draft"
type = "llm.generate"
context = [
  { resource = "todo_api", select = "operations", tag = "todos", path = "openapi.json" },
]

[params]
prompt = "prompts/draft.j2"
output_file = "draft.md"
"#;
        let node: NodeDef = toml::from_str(source).expect("resource selector should parse");
        assert_eq!(
            node.context,
            vec![ContextRef::Resource(ResourceContextRef {
                resource: "todo_api".into(),
                select: Some("operations".into()),
                tag: Some("todos".into()),
                path: Some("openapi.json".into()),
            })]
        );
        assert_eq!(node.params_json()["context"][0]["resource"], "todo_api");
    }
    #[test]
    fn node_def_deserializes_open_params() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct EchoParams {
            message: String,
            destination: String,
        }
        let source = r#"
id = "echo"
type = "demo.echo"
[params]
message = "hello"
destination = "result.txt"
"#;
        let node: NodeDef = toml::from_str(source).expect("node should parse");
        let params: EchoParams = node
            .deserialize_params()
            .expect("params should deserialize");
        assert_eq!(
            params,
            EchoParams {
                message: "hello".into(),
                destination: "result.txt".into(),
            }
        );
    }
    #[test]
    fn failure_policy_resolves_kind_and_node_override() {
        let global: FailurePolicy = toml::from_str(
            r#"
default = "fail"
[by_kind]
permission = "reject"
range = "clamp"
"#,
        )
        .expect("global failure policy should parse");
        assert_eq!(
            global.action(FailureKind::Permission),
            FailureAction::Reject
        );
        assert_eq!(global.action(FailureKind::Range), FailureAction::Clamp);
        assert_eq!(global.action(FailureKind::Schema), FailureAction::Fail);
        let node: NodeDef = toml::from_str(
            r#"
id = "generate"
type = "llm.generate"
failure = { default = "clarify", by_kind = { out_of_contract = "reject" } }
"#,
        )
        .expect("node failure override should parse");
        let override_policy = node.failure.expect("node override should exist");
        assert_eq!(
            override_policy.action(FailureKind::OutOfContract),
            FailureAction::Reject
        );
        assert_eq!(
            override_policy.action(FailureKind::Schema),
            FailureAction::Clarify
        );
    }
    #[test]
    fn command_permissions_require_explicit_isolation() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.permissions.commands.push(CommandPermission {
            bin: "tool".into(),
            args: vec![],
            purpose: "test".into(),
            isolation: None,
            image: None,
        });
        let error = manifest
            .validate()
            .expect_err("implicit host execution must be rejected");
        assert!(error.to_string().contains("must declare isolation"));
    }
    #[test]
    fn container_commands_require_digest_pinned_allowlisted_images() {
        let mut manifest = manifest_with_field(InputField {
            id: "name".into(),
            label: None,
            label_i18n: BTreeMap::new(),
            kind: FieldType::String,
            required: false,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: BTreeMap::new(),
            min_items: None,
            item_type: None,
            ..input_field_defaults()
        });
        manifest.permissions.commands.push(CommandPermission {
            bin: "tool".into(),
            args: vec![],
            purpose: "test".into(),
            isolation: Some(CommandIsolation::Container),
            image: Some("example/tool:latest".into()),
        });
        let error = manifest
            .validate()
            .expect_err("mutable image tags must be rejected");
        assert!(error.to_string().contains("pinned by digest"));
    }
    #[test]
    fn unimplemented_tool_backends_are_not_in_the_contract_type() {
        for source in [
            "[wasm]\nmodule = \"validator.wasm\"\nsha256 = \"abc\"\n",
            "[remote]\nurl = \"https://validator.example/check\"\n",
        ] {
            let error = toml::from_str::<ToolBackends>(source)
                .expect_err("unimplemented backend must be rejected during deserialization");
            assert!(error.to_string().contains("unknown field"));
        }
    }
    #[test]
    fn resource_kind_rejects_unknown_values() {
        for kind in ["OpenApi", "open-api", "acme.custom", ""] {
            let error = toml::from_str::<ResourceDef>(&format!("type = {kind:?}"))
                .expect_err("unknown resource kind must be rejected");
            assert!(error.to_string().contains("unknown variant"), "{error}");
        }
    }
    #[test]
    fn bounded_json_schema_accepts_internal_refs_and_rejects_external_refs() {
        validate_bounded_json_schema(&serde_json::json!({
            "$defs": { "value": { "type": "string" } },
            "$ref": "#/$defs/value"
        }))
        .expect("internal references must remain available");
        let error = validate_bounded_json_schema(&serde_json::json!({
            "$ref": "https://example.invalid/schema.json"
        }))
        .expect_err("external schema retrieval must not occur during validation");
        assert!(error.contains("external reference"));
    }
    #[test]
    fn bounded_json_schema_rejects_excessive_depth_and_size() {
        let mut deep = serde_json::json!({ "type": "string" });
        for _ in 0..=MAX_JSON_SCHEMA_DEPTH {
            deep = serde_json::json!({ "not": deep });
        }
        assert!(
            validate_bounded_json_schema(&deep)
                .expect_err("excessive nesting must be rejected")
                .contains("nesting")
        );
        let oversized = serde_json::json!({
            "description": "x".repeat(MAX_JSON_SCHEMA_STRING_BYTES + 1)
        });
        assert!(
            validate_bounded_json_schema(&oversized)
                .expect_err("oversized strings must be rejected")
                .contains("string")
        );
    }
    #[test]
    fn manifest_reader_stops_at_the_configured_byte_limit() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must follow the Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "qcg-contract-manifest-limit-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).expect("temporary directory must be created");
        let path = directory.join("qcg.toml");
        fs::write(&path, "0123456789").expect("temporary manifest must be written");
        let path = Utf8PathBuf::from_path_buf(path).expect("temporary path must be UTF-8");
        let error = read_manifest_with_limit(&path, Some(8))
            .expect_err("a manifest larger than the read limit must fail");
        assert!(error.to_string().contains("exceeds 8 bytes"));
        read_manifest_with_limit(&path, None).expect("unlimited manifest read must succeed");
        fs::remove_dir_all(&directory).expect("temporary directory must be removed");
    }
}
