use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{Contract, OnDeps};
use qcg_engine::TemplateService;
use qcg_server::ServerConfig;
use qcg_service::{DirectRun, LocalQcgService, direct_run_meta_dir, read_journal_events};
use reqwest::header;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use tokio::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::const_new(());

#[tokio::test]
async fn hello_template_writes_declared_artifact() {
    let run = run_fixture(
        "hello-template",
        inputs([("name", json!("qcg"))]),
        answers([]),
    )
    .await
    .expect("hello-template should run");
    assert_file_eq(&run, "README.md", "# Hello from qcg");
    assert_required_artifact(&run, "README.md");
}

#[tokio::test]
async fn file_input_is_journaled_and_materialized_in_the_workspace() {
    let file = json!({
        "name": "config.json",
        "text": "{\"enabled\":true}"
    });
    let run = run_fixture(
        "file-input",
        inputs([("config_file", file.clone())]),
        answers([]),
    )
    .await
    .expect("file-input should run");
    assert_file_eq(&run, "files/config_file/config.json", "{\"enabled\":true}");
    assert!(
        fs::read_to_string(run.join("summary.md"))
            .expect("summary should be readable")
            .contains("files/config_file/config.json")
    );
    assert_journal_has(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("run_started")
            && event["inputs"]["config_file"] == file
    });
}

#[tokio::test]
async fn transform_formats_roundtrips_structured_files() {
    let run = run_fixture("transform-formats", inputs([]), answers([]))
        .await
        .expect("transform-formats should run");
    assert!(run.join("bundle.zip").is_file());
    assert_json_field(&run, "roundtrip.json", "name", "qcg");
    assert_required_artifact(&run, "roundtrip.json");
}

#[tokio::test]
async fn parallel_wave_records_parallel_event() {
    let run = run_fixture("parallel-wave", inputs([]), answers([]))
        .await
        .expect("parallel-wave should run");
    assert_file_eq(&run, "joined.txt", "joined");
    assert_journal_has(&run, |event| {
        event.get("parallel").and_then(Value::as_bool) == Some(true)
    });
}

#[tokio::test]
async fn logical_tool_host_records_backend_resolution() {
    let run = run_fixture("logical-tool-host", inputs([]), answers([]))
        .await
        .expect("logical-tool-host should run");
    assert_file_eq(&run, "qpx.yaml", "routes: []");
    assert_journal_has(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("tool_backend_resolved")
            && event.get("backend").and_then(Value::as_str) == Some("host")
    });
}

#[tokio::test]
async fn llm_fill_retry_persists_retried_json() {
    let run = run_fixture("llm-fill-retry", inputs([]), answers([]))
        .await
        .expect("llm-fill-retry should run");
    assert_file_eq(&run, "result.json", "{\"title\":\"retry passed\"}");
    assert_journal_has(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("llm_validation_failed")
    });
}

#[tokio::test]
async fn out_of_contract_reject_fails_run() {
    let error = run_fixture("out-of-contract-reject", inputs([]), answers([]))
        .await
        .expect_err("out-of-contract reject should fail");
    assert!(error.contains("rejected as out of contract"));
}

#[tokio::test]
async fn out_of_contract_agent_reject_fails_run() {
    let error = run_fixture("out-of-contract-agent-reject", inputs([]), answers([]))
        .await
        .expect_err("out-of-contract agent reject should fail");
    assert!(error.contains("rejected as out of contract"));
}

#[tokio::test]
async fn out_of_contract_clarify_requests_user_input() {
    let error = run_fixture("out-of-contract-clarify", inputs([]), answers([]))
        .await
        .expect_err("out-of-contract clarify should wait for user input");
    assert!(error.contains("waiting for user input"));
}

#[tokio::test]
async fn out_of_contract_clamp_records_journal_and_writes_clamped_output() {
    let run = run_fixture("out-of-contract-clamp", inputs([]), answers([]))
        .await
        .expect("out-of-contract clamp should run");
    assert_file_eq(&run, "result.txt", "safe content");
    assert_journal_has(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("out_of_contract")
            && event.get("policy").and_then(Value::as_str) == Some("clamp")
    });
}

#[tokio::test]
async fn on_fail_ask_user_uses_supplied_answer() {
    let run = run_fixture(
        "on-fail-ask-user",
        inputs([]),
        answers([("check:on_fail", json!("accepted"))]),
    )
    .await
    .expect("on-fail-ask-user should run");
    assert_file_eq(&run, "decision.txt", "accepted");
}

#[tokio::test]
async fn repair_exhausted_routes_to_fallback() {
    let run = run_fixture("repair-exhausted-route", inputs([]), answers([]))
        .await
        .expect("repair-exhausted-route should run");
    assert_file_eq(&run, "fallback.txt", "repair exhausted");
    assert_journal_has(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("repair_attempt_started")
    });
}

#[tokio::test]
async fn llm_agent_fake_uses_declared_tool() {
    let run = run_fixture("llm-agent-fake", inputs([]), answers([]))
        .await
        .expect("llm-agent-fake should run");
    assert_file_eq(&run, "drafts/result.txt", "agent wrote this\n");
    assert_journal_has(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("tool_call")
            && event.get("tool").and_then(Value::as_str) == Some("write_draft")
            && event.get("ok").and_then(Value::as_bool) == Some(true)
    });
}

#[tokio::test]
async fn llm_context_includes_visible_declared_resource() {
    let run = run_fixture("llm-context", inputs([("name", json!("qcg"))]), answers([]))
        .await
        .expect("llm-context should run");
    let output = fs::read_to_string(run.join("context.txt")).expect("context output should exist");
    assert!(output.contains("<QCG_DECLARED_CONTEXT>"));
    assert!(output.contains("resources.note"));
}

#[tokio::test]
async fn llm_context_byte_limit_rejects_oversized_prompt() {
    let source = workspace_root().join("fixtures/generators/llm-context");
    let fixture = run_dir("llm-context-limit-fixture");
    let _ = fs::remove_dir_all(&fixture);
    copy_dir(&source, &fixture);
    let manifest_path = fixture.join("qcg.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("manifest should be readable");
    fs::write(
        &manifest_path,
        manifest.replace(
            "max_tokens = 512",
            "max_tokens = 512\nmax_context_bytes = 32",
        ),
    )
    .expect("manifest should be writable");

    let error = run_generator(
        fixture,
        "llm-context-limit",
        inputs([("name", json!("qcg"))]),
        answers([]),
    )
    .await
    .expect_err("oversized context should fail");
    assert!(error.contains("LLM context byte limit exceeded"));
}

#[tokio::test]
async fn generator_outputs_valid_generator() {
    let run = run_generator(
        workspace_root().join("generators/generator"),
        "generator",
        inputs([]),
        answers([
            ("ask_purpose", json!({"description": "Generated by integration test"})),
            ("ask_design_mode", json!("manual")),
            ("ask_manual_form", json!({
                "generator_id": "integration-gen",
                "generator_name": "Integration Gen",
                "artifact_path": "README.md",
                "primary_step_type": "render",
                "design_json": {"input_fields": [{"id": "request", "type": "natural_language", "required": true}]},
                "include_readme": true
            })),
            ("ask_manual_render_details", json!({"artifact_content": "# Integration\n"})),
            ("ask_fs_write", json!("workspace")),
            ("ask_network", json!("none")),
            ("ask_commands", json!("none")),
            ("ask_containers", json!("none")),
            ("ask_side_effects", json!("none")),
            ("ask_secrets", json!("none")),
        ]),
    )
    .await
    .expect("generator should run");
    Contract::load(run.join("generator")).expect("generated generator should validate");
    assert!(run.join("generator/templates/artifact.txt.j2").is_file());
    assert!(run.join("generator/README.md").is_file());
    assert!(!run.join("generator/templates/README.md.j2").exists());
    assert!(run.join("generator/SKILL.md").is_file());
    assert!(
        fs::read_to_string(run.join("generator/SKILL.md"))
            .expect("generated skill should be readable")
            .contains("- `README.md`")
    );
    assert_journal_has_none(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("llm_call")
    });

    let generated_run = run_generator(
        run.join("generator"),
        "generated-render-generator",
        inputs([("request", json!("# Integration"))]),
        answers([]),
    )
    .await
    .expect("generated render generator should run");
    assert_file_eq(&generated_run, "README.md", "# Integration");
}

#[test]
fn generator_declares_design_join_and_permission_dependencies() {
    let contract = Contract::load(workspace_root().join("generators/generator"))
        .expect("bundled generator should validate");

    let fs_write = &contract.graph.nodes["ask_fs_write"];
    assert_eq!(
        fs_write.needs,
        vec![
            "design_proposal".to_string(),
            "ask_manual_form".to_string(),
            "ask_manual_render_details".to_string(),
            "ask_manual_llm_details".to_string(),
        ]
    );
    assert!(matches!(fs_write.on_deps, OnDeps::NoneFailed));

    for (node, dependency) in [
        ("ask_network", "ask_fs_write"),
        ("ask_commands", "ask_network"),
        ("ask_containers", "ask_commands"),
        ("ask_side_effects", "ask_containers"),
        ("ask_secrets", "ask_side_effects"),
    ] {
        assert_eq!(
            contract.graph.nodes[node].needs,
            vec![dependency.to_string()],
            "permission question `{node}` must explicitly follow `{dependency}`"
        );
    }
}

#[test]
fn generator_templates_use_manual_design_fields_without_llm_answers() {
    let template_service = TemplateService::default();
    let context = json!({
        "steps": {
            "ask_design_mode": { "output": "manual" },
            "ask_manual_form": {
                "output": {
                    "generator_id": "manual-generator",
                    "generator_name": "Manual Generator",
                    "primary_step_type": "llm.fill",
                    "artifact_path": "README.md",
                    "design_json": { "input_fields": [] },
                    "include_readme": false
                }
            },
            "ask_manual_llm_details": {
                "output": {
                    "prompt_instructions": "Generate the requested README.",
                    "llm_provider": "fake",
                    "llm_model": "fake"
                }
            },
            "ask_purpose": { "output": { "description": "Generate a README" } }
        }
    });

    let prompt =
        fs::read_to_string(workspace_root().join("generators/generator/templates/prompt.j2"))
            .expect("prompt template should be readable");
    let rendered_prompt = template_service
        .render_inline(&prompt, context.clone())
        .expect("manual LLM prompt should render from the manual form");
    assert!(rendered_prompt.contains("- id: manual-generator"));
    assert!(rendered_prompt.contains("- name: Manual Generator"));
    assert!(rendered_prompt.contains("- artifact: README.md"));
    assert!(rendered_prompt.contains("Generate the requested README."));

    let skill =
        fs::read_to_string(workspace_root().join("generators/generator/templates/SKILL.md.j2"))
            .expect("skill template should be readable");
    let rendered_skill = template_service
        .render_inline(&skill, context)
        .expect("manual LLM skill template should render from the manual form");
    assert!(rendered_skill.contains("- `prompts/fill.j2`"));
    assert!(rendered_skill.contains("- `schemas/fill.schema.json`"));
    assert!(!rendered_skill.contains("- `templates/artifact.txt.j2`"));
}

#[tokio::test]
async fn generator_manual_mode_never_calls_the_llm() {
    let run = run_generator(
        workspace_root().join("generators/generator"),
        "generator-write",
        inputs([]),
        answers([
            ("ask_purpose", json!({"description": "Write-mode integration coverage"})),
            ("ask_design_mode", json!("manual")),
            ("ask_manual_form", json!({
                "generator_id": "integration-write",
                "generator_name": "Integration Write",
                "artifact_path": "README.md",
                "primary_step_type": "write",
                "design_json": {"input_fields": [{"id": "request", "type": "string", "required": true}]},
                "include_readme": false
            })),
            ("ask_fs_write", json!("workspace")),
            ("ask_network", json!("none")),
            ("ask_commands", json!("none")),
            ("ask_containers", json!("none")),
            ("ask_side_effects", json!("none")),
            ("ask_secrets", json!("none")),
        ]),
    )
    .await
    .expect("generator should run");
    Contract::load(run.join("generator")).expect("generated generator should validate");

    // The write branch must reference the first declared input field.
    let manifest =
        fs::read_to_string(run.join("generator/qcg.toml")).expect("manifest should be readable");
    assert!(
        manifest.contains(r#"content = "{{ inputs.request }}""#),
        "unexpected generated manifest:\n{manifest}"
    );
    assert!(!run.join("generator/templates/artifact.txt.j2").exists());
    assert!(!run.join("generator/README.md").exists());

    assert_journal_has_none(&run, |event| {
        event.get("t").and_then(Value::as_str) == Some("llm_call")
    });

    let generated_run = run_generator(
        run.join("generator"),
        "generated-write-generator",
        inputs([("request", json!("hello write mode"))]),
        answers([]),
    )
    .await
    .expect("generated write generator should run");
    assert_file_eq(&generated_run, "README.md", "hello write mode");
}

#[tokio::test]
async fn generator_manual_llm_mode_requires_an_explicit_provider() {
    let run = run_generator(
        workspace_root().join("generators/generator"),
        "generator-manual-llm",
        inputs([]),
        answers([
            (
                "ask_purpose",
                json!({"description": "Fill a generated README with an LLM"}),
            ),
            ("ask_design_mode", json!("manual")),
            (
                "ask_manual_form",
                json!({
                    "generator_id": "integration-llm",
                    "generator_name": "Integration LLM",
                    "artifact_path": "README.md",
                    "primary_step_type": "llm.fill",
                    "design_json": {"input_fields": [{"id": "request", "type": "string", "required": true}]},
                    "include_readme": false
                }),
            ),
            (
                "ask_manual_llm_details",
                json!({
                    "prompt_instructions": "Generate a README for the request.",
                    "llm_provider": "fake",
                    "llm_model": "fake"
                }),
            ),
            ("ask_fs_write", json!("workspace")),
            ("ask_network", json!("none")),
            ("ask_commands", json!("none")),
            ("ask_containers", json!("none")),
            ("ask_side_effects", json!("none")),
            ("ask_secrets", json!("none")),
        ]),
    )
    .await
    .expect("manual llm generator should run with an explicit provider");
    Contract::load(run.join("generator")).expect("generated LLM generator should validate");

    let manifest =
        fs::read_to_string(run.join("generator/qcg.toml")).expect("manifest should be readable");
    assert!(manifest.contains("provider = \"fake\""));
    assert!(manifest.contains("type = \"llm.fill\""));
    assert!(!manifest.contains(r#"content = "{{ inputs.request }}""#));

    let generated_run = run_generator(
        run.join("generator"),
        "generated-manual-llm-generator",
        inputs([("request", json!("manual LLM request"))]),
        answers([]),
    )
    .await
    .expect("generated manual LLM generator should run");
    assert_file_eq(&generated_run, "README.md", "Generated by qcg");
}

#[tokio::test]
async fn generator_preserves_adaptive_hitl_dag() {
    let design = json!({
        "package": {
            "manifest": {
                "flow": [
                    {
                        "id": "choose_detail",
                        "type": "ask_user",
                        "params": {
                            "content": "Choose the output detail.",
                            "options": ["brief", "detailed"]
                        }
                    },
                    {
                        "id": "brief",
                        "needs": ["choose_detail"],
                        "type": "write",
                        "when": "steps.choose_detail.output == 'brief'",
                        "params": { "content": "brief", "output_file": "brief.txt" }
                    },
                    {
                        "id": "detailed",
                        "needs": ["choose_detail"],
                        "type": "write",
                        "when": "steps.choose_detail.output == 'detailed'",
                        "params": { "content": "detailed", "output_file": "detailed.txt" }
                    }
                ]
            },
            "sources": {}
        }
    });
    let run = run_generator(
        workspace_root().join("generators/generator"),
        "generator-adaptive-hitl",
        inputs([]),
        answers([
            (
                "ask_purpose",
                json!({"description": "Adaptive HITL generator"}),
            ),
            ("ask_design_mode", json!("manual")),
            (
                "ask_manual_form",
                json!({
                    "generator_id": "adaptive-hitl",
                    "generator_name": "Adaptive HITL",
                    "artifact_path": "result.txt",
                    "primary_step_type": "write",
                    "design_json": design,
                    "include_readme": false
                }),
            ),
            ("ask_fs_write", json!("workspace")),
            ("ask_network", json!("none")),
            ("ask_commands", json!("none")),
            ("ask_containers", json!("none")),
            ("ask_side_effects", json!("none")),
            ("ask_secrets", json!("none")),
        ]),
    )
    .await
    .expect("generator should preserve the proposed HITL DAG");
    Contract::load(run.join("generator")).expect("adaptive generator should validate");

    let generated_run = run_generator(
        run.join("generator"),
        "generated-adaptive-hitl",
        inputs([]),
        answers([("choose_detail", json!("detailed"))]),
    )
    .await
    .expect("generated HITL DAG should resume through the selected branch");
    assert_file_eq(&generated_run, "detailed.txt", "detailed");
    assert!(!generated_run.join("brief.txt").exists());
}

#[tokio::test]
async fn generator_llm_mode_uses_proposal() {
    let run = run_generator(
        workspace_root().join("generators/generator"),
        "generator-llm",
        inputs([]),
        answers([
            (
                "ask_purpose",
                json!({"description": "A generator that emits a friendly hello README"}),
            ),
            ("ask_design_mode", json!("llm")),
            ("ask_fs_write", json!("workspace")),
            ("ask_network", json!("none")),
            ("ask_commands", json!("none")),
            ("ask_containers", json!("none")),
            ("ask_side_effects", json!("none")),
            ("ask_secrets", json!("none")),
        ]),
    )
    .await
    .expect("generator should run");
    Contract::load(run.join("generator")).expect("generated generator should validate");
    // The packaged proposal routes through the blueprint tier.
    let manifest =
        fs::read_to_string(run.join("generator/qcg.toml")).expect("manifest should be readable");
    assert!(
        manifest.contains(r#"content = "{{ inputs.request }}""#),
        "packaged proposal should drive the generated flow:\n{manifest}"
    );

    // The reproduced generator runs end to end using the proposed stages.
    let generated_run = run_generator(
        run.join("generator"),
        "generated-packaged-generator",
        inputs([("request", json!("packaged hello"))]),
        answers([]),
    )
    .await
    .expect("generated packaged generator should run");
    assert_file_eq(&generated_run, "README.md", "packaged hello");

    // Exactly one proposal call: the design step itself.
    let events = read_journal_events(
        direct_run_meta_dir(&run)
            .parent()
            .expect("direct metadata directory has a run parent"),
    )
    .expect("journal should be readable");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("llm_call"))
            .count(),
        1,
        "llm mode should record exactly one llm_call: {events:#?}"
    );
}

#[tokio::test]
async fn generator_llm_mode_requires_a_complete_package() {
    let source = workspace_root().join("generators/generator");
    let generator = run_dir("generator-llm-no-package-source");
    let _ = fs::remove_dir_all(&generator);
    copy_dir(&source, &generator);

    let prompt_path = generator.join("prompts/design.j2");
    let prompt = fs::read_to_string(&prompt_path).expect("design prompt should be readable");
    let marker = prompt
        .find("FAKE_JSON:")
        .expect("design prompt should have a fake marker");
    let payload = json!({
        "generator_id": "no-package",
        "generator_name": "No Package"
    });
    let replacement = format!("FAKE_JSON: {}", serde_json::to_string(&payload).unwrap());
    fs::write(
        &prompt_path,
        format!("{}{}", &prompt[..marker], replacement),
    )
    .expect("design prompt should be writable");

    let error = run_generator(
        generator.clone(),
        "generator-llm-no-package",
        inputs([]),
        answers([
            (
                "ask_purpose",
                json!({"description": "Reject an incomplete proposal"}),
            ),
            ("ask_design_mode", json!("llm")),
        ]),
    )
    .await
    .expect_err("an LLM proposal without package should fail schema validation");
    assert!(error.contains("package"), "unexpected error: {error}");
    assert!(
        !run_dir("generator-llm-no-package")
            .join("generator/qcg.toml")
            .exists()
    );
    let _ = fs::remove_dir_all(&generator);
}

#[tokio::test]
async fn generator_llm_package_preserves_a_proposed_skill_source() {
    let source = workspace_root().join("generators/generator");
    let generator = run_dir("generator-llm-package-source");
    let _ = fs::remove_dir_all(&generator);
    copy_dir(&source, &generator);

    let prompt_path = generator.join("prompts/design.j2");
    let prompt = fs::read_to_string(&prompt_path).expect("design prompt should be readable");
    let marker = prompt
        .find("FAKE_JSON:")
        .expect("design prompt should have a fake marker");
    let payload = json!({
        "generator_id": "packaged-skill",
        "generator_name": "Packaged Skill",
        "package": {
            "manifest": {
                "runtime": {"command_timeout_seconds": 7},
                "budget": {"max_steps": 37},
                "journal": {"retain_days": 5},
                "flow": [{
                    "id": "emit",
                    "type": "write",
                    "artifact": {"label": "Generated README", "required": true},
                    "params": {"content": "packaged", "output_file": "README.md"}
                }]
            },
            "sources": {"SKILL.md": "# Proposed skill\n"}
        }
    });
    let replacement = format!("FAKE_JSON: {}", serde_json::to_string(&payload).unwrap());
    fs::write(
        &prompt_path,
        format!("{}{}", &prompt[..marker], replacement),
    )
    .expect("design prompt should be writable");

    let run = run_generator(
        generator.clone(),
        "generator-llm-package",
        inputs([]),
        answers([
            (
                "ask_purpose",
                json!({"description": "Preserve package sources"}),
            ),
            ("ask_design_mode", json!("llm")),
            ("ask_fs_write", json!("workspace")),
            ("ask_network", json!("none")),
            ("ask_commands", json!("none")),
            ("ask_containers", json!("none")),
            ("ask_side_effects", json!("none")),
            ("ask_secrets", json!("none")),
        ]),
    )
    .await
    .expect("a complete package proposal should run");
    let contract =
        Contract::load(run.join("generator")).expect("generated package should validate");
    assert_eq!(contract.manifest.runtime.command_timeout_seconds, 7);
    assert_eq!(contract.manifest.budget.max_steps, 37);
    assert_eq!(contract.manifest.journal.retain_days, Some(5));
    assert_eq!(
        fs::read_to_string(run.join("generator/SKILL.md")).expect("proposed skill should exist"),
        "# Proposed skill\n"
    );
    let _ = fs::remove_dir_all(&generator);
}

#[tokio::test]
async fn llm_agent_denied_rejects_undeclared_tool() {
    let error = run_fixture("llm-agent-denied", inputs([]), answers([]))
        .await
        .expect_err("undeclared agent tool should fail");
    assert!(error.contains("tool `write_outside` is not declared"));
    let run = run_dir("llm-agent-denied");
    assert!(!run.join("drafts/result.txt").exists());
}

#[tokio::test]
async fn llm_context_denied_rejects_non_visible_resource() {
    let error = run_fixture("llm-context-denied", inputs([]), answers([]))
        .await
        .expect_err("non-visible resource should fail");
    assert!(error.contains("resource `hidden` is not llm_visible"));
}

#[tokio::test]
async fn secret_leak_rejects_secret_and_keeps_journal_redacted() {
    let _guard = ENV_LOCK.lock().await;
    // SAFETY: this test serializes all environment mutation with ENV_LOCK and restores the value.
    unsafe {
        std::env::set_var("QCG_TEST_TOKEN", "super-secret-token");
    }
    let result = run_fixture("secret-leak", inputs([]), answers([])).await;
    // SAFETY: this test serializes all environment mutation with ENV_LOCK and restores the value.
    unsafe {
        std::env::remove_var("QCG_TEST_TOKEN");
    }
    let error = result.expect_err("secret-leak should fail");
    assert!(error.contains("secret `api_token`"));
    let run = run_dir("secret-leak");
    let journal =
        fs::read_to_string(direct_run_meta_dir(&run).join("journal.jsonl")).unwrap_or_default();
    assert!(!journal.contains("super-secret-token"));
    assert!(!run.join("out.txt").exists());
}

#[tokio::test]
async fn http_sse_replays_same_journal_event_sequence_for_run() {
    let runs_dir = run_dir("server-runs");
    let _ = fs::remove_dir_all(&runs_dir);
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("listener should bind: {error}"),
    };
    let port = listener
        .local_addr()
        .expect("listener should have addr")
        .port();
    let server = tokio::spawn(qcg_server::serve_with_listener(
        ServerConfig {
            generators_dir: workspace_root().join("fixtures/generators"),
            providers_path: None,
            extra_generators_dirs: vec![],
            runs_dir: runs_dir.clone(),
            cors_origins: vec![],
        },
        listener,
    ));
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let start: Value = client
        .post(format!("{base}/api/runs"))
        .json(&json!({ "generator_id": "hello-template", "inputs": { "name": "qcg" } }))
        .send()
        .await
        .expect("run should start")
        .error_for_status()
        .expect("start response should be ok")
        .json()
        .await
        .expect("start response should be JSON");
    let run_id = start["run_id"].as_str().expect("run_id should exist");
    wait_for_success(&client, &base, run_id).await;

    let artifact = client
        .get(format!("{base}/api/runs/{run_id}/artifacts/README.md"))
        .send()
        .await
        .expect("declared artifact should respond");
    assert_eq!(artifact.status(), reqwest::StatusCode::OK);
    assert_eq!(
        artifact
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/markdown")
    );
    let undeclared = client
        .get(format!(
            "{base}/api/runs/{run_id}/artifacts/not-declared.txt"
        ))
        .send()
        .await
        .expect("undeclared artifact request should respond");
    assert_eq!(undeclared.status(), reqwest::StatusCode::NOT_FOUND);

    let journal_events = read_journal_events(&runs_dir.join(run_id)).expect("journal should parse");
    let journal_types = event_types(&journal_events);
    let sse_events = read_sse_until_finished(&client, &base, run_id).await;
    let sse_types = event_types(&sse_events);
    assert_eq!(sse_types, journal_types);
    server.abort();
}

#[tokio::test]
async fn http_assets_are_declared_generic_and_metadata_is_verbatim() {
    let runs_dir = run_dir("server-assets-runs");
    let _ = fs::remove_dir_all(&runs_dir);
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("listener should bind: {error}"),
    };
    let port = listener
        .local_addr()
        .expect("listener should have addr")
        .port();
    let server = tokio::spawn(qcg_server::serve_with_listener(
        ServerConfig {
            generators_dir: workspace_root().join("fixtures/generators"),
            providers_path: None,
            extra_generators_dirs: vec![],
            runs_dir,
            cors_origins: vec!["http://demo.example".into(), "http://other.example".into()],
        },
        listener,
    ));
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let detail: Value = client
        .get(format!("{base}/api/generators/assets-demo"))
        .send()
        .await
        .expect("generator detail should respond")
        .error_for_status()
        .expect("generator detail should be successful")
        .json()
        .await
        .expect("generator detail should be JSON");
    assert_eq!(detail["assets"]["dirs"][0], "ui");
    assert_eq!(detail["assets"]["meta"]["entry"], "index.html");
    assert_eq!(detail["assets"]["meta"]["custom"]["answer"], 42);
    assert_eq!(detail["assets"]["meta"]["custom"]["enabled"], true);

    let index = client
        .get(format!(
            "{base}/api/generators/assets-demo/assets/index.html"
        ))
        .send()
        .await
        .expect("exact file asset should respond");
    assert_eq!(index.status(), reqwest::StatusCode::OK);
    assert_eq!(
        index.headers()[header::CONTENT_TYPE],
        "text/html; charset=utf-8"
    );

    let wasm = client
        .get(format!(
            "{base}/api/generators/assets-demo/assets/ui/module.wasm"
        ))
        .send()
        .await
        .expect("wasm asset should respond");
    assert_eq!(wasm.status(), reqwest::StatusCode::OK);
    assert_eq!(wasm.headers()[header::CONTENT_TYPE], "application/wasm");
    assert_eq!(wasm.headers()[header::X_FRAME_OPTIONS], "DENY");
    assert_eq!(
        wasm.headers()[header::CONTENT_SECURITY_POLICY],
        "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src 'self' data: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
    );
    assert_eq!(
        wasm.text().await.expect("wasm asset should be readable"),
        "qcg wasm asset\n"
    );

    let extensionless = client
        .get(format!(
            "{base}/api/generators/assets-demo/assets/ui/NOTICE"
        ))
        .send()
        .await
        .expect("extensionless asset should respond");
    assert_eq!(extensionless.status(), reqwest::StatusCode::OK);
    assert_eq!(
        extensionless.headers()[header::CONTENT_TYPE],
        "application/octet-stream"
    );

    let missing_in_declared_dir = client
        .get(format!(
            "{base}/api/generators/assets-demo/assets/ui/missing.bin"
        ))
        .send()
        .await
        .expect("missing directory asset should respond");
    assert_eq!(
        missing_in_declared_dir.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    let undeclared = client
        .get(format!(
            "{base}/api/generators/assets-demo/assets/undeclared.bin"
        ))
        .send()
        .await
        .expect("undeclared asset should respond");
    assert_eq!(undeclared.status(), reqwest::StatusCode::NOT_FOUND);

    let traversal = client
        .get(format!(
            "{base}/api/generators/assets-demo/assets/ui/%2E%2E%2Fqcg.toml"
        ))
        .send()
        .await
        .expect("traversal asset should respond");
    assert_eq!(traversal.status(), reqwest::StatusCode::BAD_REQUEST);

    let preflight = client
        .request(reqwest::Method::OPTIONS, format!("{base}/api/runs"))
        .header(header::ORIGIN, "http://other.example")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            "content-type, idempotency-key",
        )
        .send()
        .await
        .expect("CORS preflight should respond");
    assert!(preflight.status().is_success());
    assert_eq!(
        preflight.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        "http://other.example"
    );
    let allowed_headers = preflight
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .and_then(|value| value.to_str().ok())
        .expect("CORS should list allowed headers");
    assert!(allowed_headers.contains("content-type"));
    assert!(allowed_headers.contains("idempotency-key"));
    assert!(
        !preflight
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
    );
    server.abort();
}

#[tokio::test]
async fn http_server_is_unauthenticated_and_writes_need_no_extra_headers() {
    let runs_dir = run_dir("server-unauthenticated-runs");
    let _ = fs::remove_dir_all(&runs_dir);
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("listener should bind: {error}"),
    };
    let port = listener
        .local_addr()
        .expect("listener should have addr")
        .port();
    let server = tokio::spawn(qcg_server::serve_with_listener(
        ServerConfig {
            generators_dir: workspace_root().join("fixtures/generators"),
            providers_path: None,
            extra_generators_dirs: vec![],
            runs_dir: runs_dir.clone(),
            cors_origins: vec![],
        },
        listener,
    ));
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let generators = client
        .get(format!("{base}/api/generators"))
        .send()
        .await
        .expect("unauthenticated request should respond");
    assert_eq!(generators.status(), reqwest::StatusCode::OK);
    assert_eq!(
        generators
            .headers()
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .and_then(|value| value.to_str().ok()),
        Some("nosniff")
    );
    assert_eq!(
        generators
            .headers()
            .get(header::X_FRAME_OPTIONS)
            .and_then(|value| value.to_str().ok()),
        Some("DENY")
    );

    let start = client
        .post(format!("{base}/api/runs"))
        .json(&json!({ "generator_id": "hello-template", "inputs": { "name": "qcg" } }))
        .send()
        .await
        .expect("write without authentication headers should respond");
    assert_eq!(start.status(), reqwest::StatusCode::CREATED);

    let malformed_file = client
        .post(format!("{base}/api/runs"))
        .json(&json!({
            "generator_id": "file-input",
            "inputs": { "config_file": "plain-string-is-invalid" }
        }))
        .send()
        .await
        .expect("malformed inline file should respond");
    assert_eq!(
        malformed_file.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    let inline_file = client
        .post(format!("{base}/api/runs"))
        .json(&json!({
            "generator_id": "file-input",
            "inputs": {
                "config_file": { "name": "http.json", "text": "{\"ok\":true}" }
            }
        }))
        .send()
        .await
        .expect("inline file run should respond");
    assert_eq!(inline_file.status(), reqwest::StatusCode::CREATED);
    let inline_snapshot: Value = inline_file
        .json()
        .await
        .expect("inline file snapshot should be JSON");
    let inline_run_id = inline_snapshot["run_id"]
        .as_str()
        .expect("inline file run id should be present");
    wait_for_success(&client, &base, inline_run_id).await;
    assert_eq!(
        fs::read_to_string(
            runs_dir
                .join(inline_run_id)
                .join("workspace/files/config_file/http.json")
        )
        .expect("HTTP file input should be materialized"),
        "{\"ok\":true}"
    );
    server.abort();
}

#[tokio::test]
async fn direct_run_events_match_journal_event_sequence() {
    let output_dir = run_dir("direct-run-events");
    let _ = fs::remove_dir_all(&output_dir);
    fs::create_dir_all(&output_dir).expect("run directory should be creatable");
    let service = LocalQcgService::new(
        workspace_root().join("fixtures/generators"),
        output_dir
            .parent()
            .expect("run dir should have parent")
            .to_path_buf(),
        None,
    )
    .expect("service should initialize");
    let result = service
        .run_generator_path_with_events(DirectRun {
            generator_path: workspace_root().join("fixtures/generators/hello-template"),
            inputs: inputs([("name", json!("qcg"))]),
            output_dir: output_dir.clone(),
            json_events: false,
            interactive: false,
            answers: answers([]),
            confirmations: BTreeMap::new(),
            llm_seed_override: None,
        })
        .await
        .expect("direct run should succeed");
    let event_values = result
        .events
        .iter()
        .map(|event| serde_json::to_value(event).expect("event should serialize"))
        .collect::<Vec<_>>();
    let journal_events = read_journal_events(
        direct_run_meta_dir(&output_dir)
            .parent()
            .expect("direct metadata directory has a run parent"),
    )
    .expect("journal should parse");
    assert_eq!(event_types(&event_values), event_types(&journal_events));
}

#[tokio::test]
async fn foreach_parallelism_preserves_all_iterations_and_truncates_at_budget() {
    let output_dir = run_dir("foreach-parallel-budget");
    let _ = fs::remove_dir_all(&output_dir);
    fs::create_dir_all(&output_dir).expect("run directory should be creatable");
    let service = LocalQcgService::new(
        workspace_root().join("fixtures/generators"),
        output_dir
            .parent()
            .expect("run dir should have parent")
            .to_path_buf(),
        None,
    )
    .expect("service should initialize");
    let sites = (0..12)
        .map(|index| Value::String(format!("site-{index}")))
        .collect::<Vec<_>>();
    let result = service
        .run_generator_path_with_events(DirectRun {
            generator_path: workspace_root().join("fixtures/generators/foreach-sites"),
            inputs: inputs([("sites", Value::Array(sites))]),
            output_dir: output_dir.clone(),
            json_events: false,
            interactive: false,
            answers: answers([]),
            confirmations: BTreeMap::new(),
            llm_seed_override: None,
        })
        .await
        .expect("foreach run should succeed");
    let events = result
        .events
        .iter()
        .map(|event| serde_json::to_value(event).expect("event should serialize"))
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "foreach_iteration")
            .count(),
        10
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "foreach_budget_exhausted")
            .count(),
        1
    );
    for index in 0..10 {
        assert!(output_dir.join(format!("sites/site-{index}.txt")).is_file());
    }
    assert!(!output_dir.join("sites/site-10.txt").exists());
}

async fn run_fixture(
    name: &str,
    inputs: BTreeMap<String, Value>,
    answers: BTreeMap<String, Value>,
) -> Result<Utf8PathBuf, String> {
    run_generator(
        workspace_root().join("fixtures/generators").join(name),
        name,
        inputs,
        answers,
    )
    .await
}

async fn run_generator(
    generator_path: Utf8PathBuf,
    name: &str,
    inputs: BTreeMap<String, Value>,
    answers: BTreeMap<String, Value>,
) -> Result<Utf8PathBuf, String> {
    let output_dir = run_dir(name);
    let _ = fs::remove_dir_all(&output_dir);
    fs::create_dir_all(&output_dir).expect("run directory should be creatable");
    let generators_dir = generator_path
        .parent()
        .expect("generator path should have a parent")
        .to_path_buf();
    let service = LocalQcgService::new(
        generators_dir,
        output_dir
            .parent()
            .expect("run dir should have parent")
            .to_path_buf(),
        None,
    )
    .expect("service should initialize");
    service
        .run_generator_path(DirectRun {
            generator_path,
            inputs,
            output_dir: output_dir.clone(),
            json_events: false,
            interactive: false,
            answers,
            confirmations: BTreeMap::new(),
            llm_seed_override: None,
        })
        .await
        .map(|_| output_dir)
        .map_err(|error| error.to_string())
}

fn inputs<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn answers<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    inputs(items)
}

fn workspace_root() -> Utf8PathBuf {
    Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("integration crate should live under tests/integration")
        .to_path_buf()
}

fn run_dir(name: &str) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(
        std::env::temp_dir()
            .join(format!("qcg-integration-{}", std::process::id()))
            .join(name),
    )
    .expect("temporary path should be UTF-8")
}

fn copy_dir(source: &Utf8Path, target: &Utf8Path) {
    fs::create_dir_all(target).expect("target directory should be creatable");
    for entry in fs::read_dir(source).expect("source directory should be readable") {
        let entry = entry.expect("source entry should be readable");
        let source_path = Utf8PathBuf::from_path_buf(entry.path()).expect("path should be UTF-8");
        let target_path = target.join(source_path.file_name().expect("entry should have a name"));
        if source_path.is_dir() {
            copy_dir(&source_path, &target_path);
        } else {
            fs::copy(&source_path, &target_path).expect("fixture file should be copied");
        }
    }
}

fn assert_file_eq(run: &Utf8Path, path: &str, expected: &str) {
    let actual = fs::read_to_string(run.join(path)).expect("artifact should be readable");
    assert_eq!(actual, expected);
}

fn assert_json_field(run: &Utf8Path, path: &str, field: &str, expected: &str) {
    let text = fs::read_to_string(run.join(path)).expect("json artifact should be readable");
    let value: Value = serde_json::from_str(&text).expect("artifact should be JSON");
    assert_eq!(value.get(field).and_then(Value::as_str), Some(expected));
}

fn assert_required_artifact(run: &Utf8Path, path: &str) {
    let manifest = qcg_engine::read_output_manifest(&direct_run_meta_dir(run))
        .expect("output manifest should exist");
    assert!(
        manifest
            .artifacts
            .iter()
            .any(|artifact| artifact.path == path && artifact.required)
    );
}

fn assert_journal_has(run: &Utf8Path, predicate: impl Fn(&Value) -> bool) {
    let events = read_journal_events(
        direct_run_meta_dir(run)
            .parent()
            .expect("direct metadata directory has a run parent"),
    )
    .expect("journal should be readable");
    assert!(
        events.iter().any(predicate),
        "expected event was not found in journal: {events:#?}"
    );
}

fn assert_journal_has_none(run: &Utf8Path, predicate: impl Fn(&Value) -> bool) {
    let events = read_journal_events(
        direct_run_meta_dir(run)
            .parent()
            .expect("direct metadata directory has a run parent"),
    )
    .expect("journal should be readable");
    assert!(
        events.iter().all(|event| !predicate(event)),
        "forbidden event was found in journal: {events:#?}"
    );
}

async fn wait_for_success(client: &reqwest::Client, base: &str, run_id: &str) {
    for _ in 0..50 {
        let snapshot: Value = client
            .get(format!("{base}/api/runs/{run_id}"))
            .send()
            .await
            .expect("snapshot request should succeed")
            .error_for_status()
            .expect("snapshot response should be ok")
            .json()
            .await
            .expect("snapshot should be JSON");
        if snapshot["state"].as_str() == Some("succeeded") {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("run did not finish");
}

async fn read_sse_until_finished(client: &reqwest::Client, base: &str, run_id: &str) -> Vec<Value> {
    let response = client
        .get(format!("{base}/api/runs/{run_id}/events"))
        .send()
        .await
        .expect("SSE request should succeed")
        .error_for_status()
        .expect("SSE response should be ok");
    let mut buffer = String::new();
    let mut events = Vec::new();
    let mut response = response;
    while let Some(chunk) =
        tokio::time::timeout(std::time::Duration::from_secs(2), response.chunk())
            .await
            .expect("SSE should produce run events")
            .expect("SSE chunk should be readable")
    {
        buffer.push_str(std::str::from_utf8(&chunk).expect("SSE should be UTF-8"));
        while let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_string();
            buffer.drain(..index + 2);
            for line in frame.lines() {
                if let Some(data) = line.strip_prefix("data: ") {
                    let event: Value =
                        serde_json::from_str(data).expect("SSE event should be JSON");
                    let done = event
                        .get("kind")
                        .or_else(|| event.get("t"))
                        .and_then(Value::as_str)
                        == Some("run_finished");
                    events.push(event);
                    if done {
                        return events;
                    }
                }
            }
        }
    }
    panic!("SSE ended before run_finished");
}

fn event_types(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|event| {
            event
                .get("kind")
                .or_else(|| event.get("t"))
                .and_then(Value::as_str)
        })
        .collect()
}
