use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::Contract;
use qcg_service::{DirectRun, LocalQcgService};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

fn workspace_root() -> Utf8PathBuf {
    Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("integration crate should live under tests/integration")
        .to_path_buf()
}

fn run_dir(name: &str) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-self-hosting-{}-{}",
        std::process::id(),
        name
    )))
    .expect("temporary path should be UTF-8")
}

fn inputs<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    items.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn answers<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    inputs(items)
}

async fn run_generator(
    generator: Utf8PathBuf,
    label: &str,
    values: BTreeMap<String, Value>,
    reply: BTreeMap<String, Value>,
) -> Result<Utf8PathBuf, String> {
    let runs = run_dir(label).join("runs");
    let service = LocalQcgService::new(workspace_root().join("fixtures/generators"), runs, None)
        .map_err(|error| error.to_string())?;
    let manifest = service
        .run_generator_path(DirectRun {
            generator_path: generator,
            inputs: values,
            output_dir: run_dir(label),
            json_events: false,
            interactive: false,
            answers: reply,
            confirmations: BTreeMap::new(),
            llm_seed_override: None,
        })
        .await
        .map_err(|error| error.to_string())?;
    let _ = manifest;
    Ok(run_dir(label))
}

/// The blueprint is deliberate test input, not a production package-discovery
/// mechanism: it describes the intended generator and its reproduction
/// sources. Asset directories are excluded so a locally built UI cannot
/// contaminate the contract used for the depth-2 reproduction check.
fn generator_blueprint() -> Value {
    let root = workspace_root().join("generators/generator");
    let text = fs::read_to_string(root.join("qcg.toml")).expect("builder qcg.toml");
    let value: toml::Value = toml::from_str(&text).expect("builder manifest should parse");
    let mut manifest = serde_json::to_value(value).expect("manifest converts to JSON");
    if let Some(object) = manifest.as_object_mut() {
        object.remove("generator");
        object.remove("permissions");
    }
    let asset_dirs = manifest
        .get("assets")
        .and_then(Value::as_object)
        .and_then(|assets| assets.get("dirs"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(PathBuf::from)
        .collect::<Vec<_>>();

    let mut sources = BTreeMap::new();
    for entry in walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(&root)
            .expect("file lives under builder root");
        if relative == std::path::Path::new("qcg.toml") {
            continue;
        }
        if asset_dirs
            .iter()
            .any(|asset_dir| relative.starts_with(asset_dir))
        {
            continue;
        }
        let content = fs::read_to_string(path).expect("builder sources are UTF-8 text files");
        sources.insert(
            relative.to_string_lossy().replace('\\', "/"),
            Value::String(content),
        );
    }

    json!({
        "input_fields": [],
        "package": {
            "manifest": manifest,
            "sources": sources,
        },
    })
}

fn builder_answers() -> BTreeMap<String, Value> {
    answers([
        ("ask_fs_write", json!("workspace")),
        ("ask_network", json!("none")),
        ("ask_commands", json!("none")),
        ("ask_containers", json!("none")),
        ("ask_side_effects", json!("none")),
        ("ask_secrets", json!("none")),
    ])
}

fn builder_answers_with<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    let mut result = builder_answers();
    result.extend(answers(items));
    result
}

fn manifest_as_json(path: &Utf8Path) -> Value {
    let text = fs::read_to_string(path).expect("manifest should be readable");
    let value: toml::Value = toml::from_str(&text).expect("manifest should parse as TOML");
    serde_json::to_value(value).expect("TOML converts to JSON")
}

fn file_set(root: &Utf8Path) -> Vec<String> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
    {
        if entry.file_type().is_file() {
            files.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .expect("entry lives under root")
                    .to_string_lossy()
                    .to_string(),
            );
        }
    }
    files.sort();
    files
}

#[tokio::test]
async fn generator_reproduces_itself_and_the_clone_reproduces_again() {
    let blueprint = generator_blueprint();

    // Depth 1: the bundled builder reproduces itself from the blue print.
    let clone_a = run_generator(
        workspace_root().join("generators/generator"),
        "clone-a",
        inputs([]),
        builder_answers_with([
            (
                "ask_purpose",
                json!({"description": "Reproduced \"generator\""}),
            ),
            ("ask_design_mode", json!("manual")),
            (
                "ask_manual_form",
                json!({
                    "generator_id": "self-host-a",
                    "generator_name": "Self Host A",
                    "artifact_path": "README.md",
                    "primary_step_type": "write",
                    "design_json": blueprint.clone(),
                    "include_readme": false
                }),
            ),
        ]),
    )
    .await
    .expect("depth 1 reproduction should succeed");

    let clone_a_root = clone_a.join("generator");
    Contract::load(&clone_a_root).expect("reproduced builder should validate");

    // Reproduction is not duplication: every source must have been written by
    // the declared foreach/write steps, byte for byte from the blue print.
    let blueprint_sources = blueprint["package"]["sources"]
        .as_object()
        .expect("sources");
    assert!(!blueprint_sources.is_empty(), "blueprint carries sources");
    for (path, source) in blueprint_sources {
        let expected = source.as_str().expect("source content");
        let actual = fs::read_to_string(clone_a_root.join(path))
            .unwrap_or_else(|error| panic!("source `{path}` should exist: {error}"));
        assert_eq!(
            actual, expected,
            "source `{path}` must match the blue print"
        );
    }

    // Capability parity: the reproduced flow is the original flow.
    let original = manifest_as_json(&workspace_root().join("generators/generator/qcg.toml"));
    let reproduced = manifest_as_json(&clone_a_root.join("qcg.toml"));
    for section in ["flow", "blocks", "outputs", "inputs", "assets", "llm"] {
        assert_eq!(
            original.get(section),
            reproduced.get(section),
            "section `{section}` must reproduce exactly"
        );
    }
    // Ask-driven sections are intentionally owned by the operator answers.
    assert!(reproduced.get("generator").is_some());
    assert!(reproduced.get("permissions").is_some());

    // Depth 2: the reproduced builder consumes the same blue print and
    // reproduces again. Same inputs make the manifests byte identical.
    let clone_b = run_generator(
        clone_a_root.clone(),
        "clone-b",
        inputs([]),
        builder_answers_with([
            (
                "ask_purpose",
                json!({"description": "Reproduced \"generator\""}),
            ),
            ("ask_design_mode", json!("manual")),
            (
                "ask_manual_form",
                json!({
                    "generator_id": "self-host-a",
                    "generator_name": "Self Host A",
                    "artifact_path": "README.md",
                    "primary_step_type": "write",
                    "design_json": blueprint,
                    "include_readme": false
                }),
            ),
        ]),
    )
    .await
    .expect("depth 2 reproduction should succeed");

    let clone_b_root = clone_b.join("generator");
    Contract::load(&clone_b_root).expect("depth 2 builder should validate");
    assert_file_eq(
        &clone_a_root.join("qcg.toml"),
        &clone_b_root.join("qcg.toml"),
    );
    assert_eq!(
        file_set(&clone_a_root),
        file_set(&clone_b_root),
        "depth 2 clone must carry the identical file set"
    );

    for dir in ["clone-a", "clone-b", "runs"] {
        let _ = fs::remove_dir_all(run_dir(dir));
    }
}

fn assert_file_eq(left: &Utf8Path, right: &Utf8Path) {
    let left_text = fs::read_to_string(left).expect("left file should be readable");
    let right_text = fs::read_to_string(right).expect("right file should be readable");
    assert_eq!(left_text, right_text);
}

#[tokio::test]
async fn llm_mode_accepts_a_packaged_proposal() {
    // The proposal schema requires a complete `package` tier; fake keeps CI deterministic.
    let run = run_generator(
        workspace_root().join("generators/generator"),
        "clone-llm-minimal",
        inputs([]),
        builder_answers_with([
            (
                "ask_purpose",
                json!({"description": "LLM tier carrying a package"}),
            ),
            ("ask_design_mode", json!("llm")),
        ]),
    )
    .await
    .expect("packaged llm proposal should build");

    Contract::load(run.join("generator")).expect("generated package should validate");
}
