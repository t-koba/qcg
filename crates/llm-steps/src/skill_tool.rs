use contract::{Contract, NodeDef, ResourceKind, ToolDecl};
use engine::{
    ResourceSelector, ResultExt, StepContext, StepError, discover_skill_docs, select_resource,
};
use llm::ToolSpec;
use serde_json::{Value, json};

const MAX_LISTED_SKILL_RESOURCES: usize = 256;

pub(crate) struct SkillBinding {
    pub(crate) name: String,
    pub(crate) resource: String,
    pub(crate) library_dir: Option<String>,
}

pub(crate) fn validate_skill_tool(
    node: &NodeDef,
    contract: &Contract,
    tool: &ToolDecl,
) -> Result<(), StepError> {
    let ToolDecl::Skill {
        name,
        resources,
        max_calls,
        ..
    } = tool
    else {
        return Ok(());
    };
    if resources.is_empty() {
        return Err(StepError::failed(
            &node.id,
            format!("skill tool `{name}` requires at least one resource"),
        ));
    }
    if *max_calls == 0 {
        return Err(StepError::failed(
            &node.id,
            format!("skill tool `{name}` max_calls must be greater than zero"),
        ));
    }
    let bindings = collect_bindings(contract, resources)
        .map_err(|message| StepError::failed(&node.id, message))?;
    let mut names = std::collections::BTreeSet::new();
    for binding in &bindings {
        if !names.insert(binding.name.clone()) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "skill tool `{name}` binds duplicate skill name `{}`",
                    binding.name
                ),
            ));
        }
    }
    if bindings.is_empty() {
        return Err(StepError::failed(
            &node.id,
            format!("skill tool `{name}` resolves no skills"),
        ));
    }
    Ok(())
}

pub(crate) fn skill_tool_spec(
    ctx: &StepContext<'_>,
    tool: &ToolDecl,
) -> Result<ToolSpec, StepError> {
    let ToolDecl::Skill {
        name,
        description,
        resources,
        ..
    } = tool
    else {
        return Err(StepError::failed(
            "",
            format!("tool `{}` is not a skill tool", tool.name()),
        ));
    };
    let bindings = collect_bindings(&ctx.run.contract, resources)
        .map_err(|message| StepError::failed(name, message))?;
    let mut catalog = Vec::new();
    for binding in &bindings {
        let description = binding_description(ctx, binding)?;
        catalog.push(format!("- {}: {description}", binding.name));
    }
    let mut catalog_text = format!(
        "Activate a declared Agent Skill and load its instructions. \
         Bundled resources are listed but loaded only when `file` is requested.\n\nAvailable skills:\n{}",
        catalog.join("\n")
    );
    catalog_text.push_str(
        "\n\nUse this tool when a task matches a skill description. Relative paths in a skill resolve against the skill directory; pass them through `file`.",
    );
    let names: Vec<&str> = bindings
        .iter()
        .map(|binding| binding.name.as_str())
        .collect();
    let base = description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("{value}\n\n{catalog_text}"))
        .unwrap_or(catalog_text);
    Ok(ToolSpec {
        name: name.clone(),
        description: base,
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["skill"],
            "properties": {
                "skill": {
                    "type": "string",
                    "enum": names,
                    "description": "Skill name from the catalog."
                },
                "file": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Optional skill-relative file path to read, for example `references/guide.md`."
                }
            }
        }),
    })
}

pub(crate) fn execute_skill_tool(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    tool_name: &str,
    resources: &[String],
    args: &Value,
    activated: &mut std::collections::BTreeSet<String>,
) -> Result<Value, StepError> {
    let skill = args
        .get("skill")
        .and_then(Value::as_str)
        .ok_or_else(|| StepError::failed(&node.id, format!("tool `{tool_name}` requires skill")))?;
    if skill.trim().is_empty() {
        return Err(StepError::failed(
            &node.id,
            format!("tool `{tool_name}` skill must not be empty"),
        ));
    }
    let bindings = collect_bindings(&ctx.run.contract, resources)
        .map_err(|message| StepError::failed(&node.id, message))?;
    let binding = bindings
        .iter()
        .find(|binding| binding.name == skill)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("tool `{tool_name}` has no declared skill `{skill}`"),
            )
        })?;
    let resource = ctx
        .run
        .contract
        .manifest
        .resources
        .get(&binding.resource)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("skill resource `{}` is not declared", binding.resource),
            )
        })?;
    // Progressive disclosure dedup: instructions are injected once per agent
    // run, so a repeated activation returns a short marker instead of the
    // same content again. A `file` request stays meaningful after activation.
    let key = format!("{}::{}", binding.resource, binding.name);
    let already_active = activated.contains(&key);
    let file = args.get("file").and_then(Value::as_str);
    if already_active && file.is_none() {
        return Ok(json!({
            "skill": binding.name,
            "already_active": true,
        }));
    }
    if !already_active {
        activated.insert(key);
    }
    let mut result = json!({
        "skill": binding.name,
        "already_active": already_active,
    });
    if !already_active {
        let instructions_selector = match &binding.library_dir {
            Some(dir) => ResourceSelector::Named(format!("instructions/{dir}")),
            None => ResourceSelector::Named("instructions".into()),
        };
        let instructions = select_resource(
            ctx.run,
            &binding.resource,
            resource,
            Some(&instructions_selector),
        )
        .step_err(&node.id)?;
        if let Some(object) = result.as_object_mut() {
            object.insert("instructions".into(), instructions.into());
        }
    }
    if !already_active {
        let tree_selector = match &binding.library_dir {
            Some(dir) => ResourceSelector::Named(format!("tree/{dir}")),
            None => ResourceSelector::Named("tree".into()),
        };
        let tree = select_resource(ctx.run, &binding.resource, resource, Some(&tree_selector))
            .step_err(&node.id)?;
        let resources = skill_resource_paths(&tree);
        if let Some(object) = result.as_object_mut() {
            object.insert("resources".into(), json!(resources));
        }
    }
    if let Some(file) = file {
        let selector = match &binding.library_dir {
            Some(dir) => ResourceSelector::File {
                path: format!("{dir}/{file}"),
            },
            None => ResourceSelector::File {
                path: file.to_string(),
            },
        };
        let content = select_resource(ctx.run, &binding.resource, resource, Some(&selector))
            .step_err(&node.id)?;
        if let Some(object) = result.as_object_mut() {
            object.insert("file".into(), json!({ "path": file, "content": content }));
        }
    }
    Ok(result)
}

fn binding_description(ctx: &StepContext<'_>, binding: &SkillBinding) -> Result<String, StepError> {
    let resource = ctx
        .run
        .contract
        .manifest
        .resources
        .get(&binding.resource)
        .ok_or_else(|| {
            StepError::failed(
                "",
                format!("skill resource `{}` is not declared", binding.resource),
            )
        })?;
    let selector = match &binding.library_dir {
        Some(dir) => ResourceSelector::Named(format!("meta/{dir}")),
        None => ResourceSelector::Named("meta".into()),
    };
    let meta = select_resource(ctx.run, &binding.resource, resource, Some(&selector))
        .step_err("skill_catalog")?;
    let value: Value = serde_json::from_str(&meta).map_err(|error| {
        StepError::failed(
            "skill_catalog",
            format!("skill meta for `{}` is not JSON: {error}", binding.name),
        )
    })?;
    Ok(value
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}

fn skill_resource_paths(tree: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<Value>(tree) else {
        return Vec::new();
    };
    let Some(files) = value.get("files").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut paths: Vec<String> = files
        .iter()
        .filter_map(|file| file.get("path").and_then(Value::as_str))
        .filter(|path| *path != "SKILL.md")
        .map(str::to_string)
        .collect();
    paths.sort();
    paths.truncate(MAX_LISTED_SKILL_RESOURCES);
    paths
}

fn collect_bindings(
    contract: &Contract,
    resources: &[String],
) -> Result<Vec<SkillBinding>, String> {
    let mut bindings = Vec::new();
    for resource_name in resources {
        let resource = contract
            .manifest
            .resources
            .get(resource_name)
            .ok_or_else(|| format!("skill tool references unknown resource `{resource_name}`"))?;
        if !resource.llm_visible {
            return Err(format!(
                "skill resource `{resource_name}` must set llm_visible = true"
            ));
        }
        let is_library = match resource.kind {
            ResourceKind::Skill => false,
            ResourceKind::SkillLibrary => true,
            _ => {
                return Err(format!(
                    "skill tool resource `{resource_name}` must use type `skill` or `skill_library`"
                ));
            }
        };
        let path = resource.path.as_deref().ok_or_else(|| {
            if is_library {
                format!("skill_library resource `{resource_name}` requires path")
            } else {
                format!("skill resource `{resource_name}` requires path")
            }
        })?;
        let root = contract.resolve_package_path(path).map_err(|error| {
            if is_library {
                format!("skill_library resource `{resource_name}` path is invalid: {error}")
            } else {
                format!("skill resource `{resource_name}` path is invalid: {error}")
            }
        })?;
        // All discovery reads go through the engine's bounded no-follow
        // path so configured `ResourceDef.params` limits apply here exactly
        // as they do in snapshot/select; no direct filesystem reads remain.
        let discovered = discover_skill_docs(resource_name, resource, &root)
            .map_err(|error| error.to_string())?;
        for item in discovered {
            let name = item
                .library_dir
                .clone()
                .unwrap_or_else(|| item.doc.name.clone());
            bindings.push(SkillBinding {
                name,
                resource: resource_name.clone(),
                library_dir: item.library_dir,
            });
        }
    }
    Ok(bindings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use contract::Contract;
    use engine::{JournalWriter, RunContext};
    use std::collections::BTreeMap;

    fn test_node() -> NodeDef {
        NodeDef {
            id: "agent".into(),
            kind: contract::StepType::literal("llm.agent"),
            needs: vec![],
            when: None,
            on_deps: contract::OnDeps::AllSucceeded,
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        }
    }

    fn skill_tool(resources: Vec<String>) -> ToolDecl {
        ToolDecl::Skill {
            name: "skills".into(),
            description: None,
            resources,
            max_calls: 1,
        }
    }

    fn write_skill(root: &camino::Utf8Path, dir: &str, body: &str) -> camino::Utf8PathBuf {
        let skill_dir = root.join(dir);
        std::fs::create_dir_all(&skill_dir).expect("skill directory should be created");
        std::fs::write(skill_dir.join("SKILL.md"), body).expect("skill file should be written");
        skill_dir
    }

    fn temp_root(name: &str) -> camino::Utf8PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "llm-skill-tool-{name}-{}-{nanos}",
            std::process::id(),
        )))
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("fixture root should be created");
        root
    }

    fn contract_with_skill(root: &camino::Utf8Path, resource_toml: &str) -> Contract {
        std::fs::write(
            root.join(contract::MANIFEST_FILE),
            format!(
                r#"
[generator]
id = "skill-test"
name = "Skill Test"
version = "0.1.0"

[llm]
model = {{ provider = "fake", model = "fake" }}
max_tokens = 2048

[[flow]]
id = "gen"
type = "llm.generate"

[flow.params]
prompt = "prompt.j2"

{resource_toml}
"#
            ),
        )
        .expect("manifest should be written");
        std::fs::write(root.join("prompt.j2"), "Generate a bounded result.")
            .expect("prompt should be written");
        Contract::load(root.to_path_buf()).expect("contract should load")
    }

    fn set_selected_bytes(contract: &mut Contract, resource: &str, bound: Option<usize>) {
        let entry = contract
            .manifest
            .resources
            .get_mut(resource)
            .expect("skill resource should be declared");
        if let Some(bound) = bound {
            entry
                .params
                .insert("max_selected_bytes".into(), json!(bound));
        } else {
            entry.params.remove("max_selected_bytes");
        }
    }

    fn spec_context<'a>(
        run: &'a RunContext,
        journal: &'a JournalWriter,
        vars: &'a mut contract::ValueBag,
    ) -> StepContext<'a> {
        StepContext {
            run,
            journal,
            vars,
            llm: None,
        }
    }

    #[test]
    fn skill_validation_enforces_an_explicit_skill_doc_bound() {
        let root = temp_root("validation-bound");
        write_skill(
            &root,
            "demo",
            "---\nname: demo\ndescription: Demo skill.\n---\nBody.\n",
        );
        let mut contract = contract_with_skill(
            &root,
            r#"
[resources.demo]
type = "skill"
path = "demo"
llm_visible = true
"#,
        );
        let node = test_node();
        let tool = skill_tool(vec!["demo".into()]);
        validate_skill_tool(&node, &contract, &tool).expect("unbounded skill should validate");
        let len = std::fs::read(root.join("demo/SKILL.md"))
            .expect("skill file should be readable")
            .len();
        set_selected_bytes(&mut contract, "demo", Some(len));
        validate_skill_tool(&node, &contract, &tool)
            .expect("exact-bound skill should still validate");
        set_selected_bytes(&mut contract, "demo", Some(len - 1));
        let error = validate_skill_tool(&node, &contract, &tool)
            .expect_err("oversized SKILL.md must fail validation at the configured bound");
        assert!(error.to_string().contains("max_bytes"), "{error}");
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn skill_library_validation_enforces_entry_bounds() {
        let root = temp_root("library-entry-bound");
        write_skill(
            &root,
            "skills/alpha",
            "---\nname: alpha\ndescription: Alpha skill.\n---\nBody.\n",
        );
        std::fs::create_dir_all(root.join("skills/empty"))
            .expect("empty library entry should be created");
        std::fs::write(root.join("skills/notes.txt"), "not a skill")
            .expect("non-skill entry should be written");
        let mut contract = contract_with_skill(
            &root,
            r#"
[resources.lib]
type = "skill_library"
path = "skills"
llm_visible = true
"#,
        );
        let resource = contract
            .manifest
            .resources
            .get_mut("lib")
            .expect("library should be declared");
        resource.params.insert("max_entries".into(), json!(4));
        let node = test_node();
        let tool = skill_tool(vec!["lib".into()]);
        validate_skill_tool(&node, &contract, &tool)
            .expect("exact entry bound should validate, including ignored entries");
        contract
            .manifest
            .resources
            .get_mut("lib")
            .expect("library should be declared")
            .params
            .insert("max_entries".into(), json!(3));
        let error = validate_skill_tool(&node, &contract, &tool)
            .expect_err("empty and non-skill entries must count toward max_entries");
        assert!(error.to_string().contains("max_entries"), "{error}");
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }

    #[test]
    fn skill_catalog_spec_enforces_the_same_explicit_bound() {
        let root = temp_root("spec-bound");
        write_skill(
            &root,
            "demo",
            "---\nname: demo\ndescription: Demo skill.\n---\nBody.\n",
        );
        let mut contract = contract_with_skill(
            &root,
            r#"
[resources.demo]
type = "skill"
path = "demo"
llm_visible = true
"#,
        );
        let len = std::fs::read(root.join("demo/SKILL.md"))
            .expect("skill file should be readable")
            .len();
        let tool = skill_tool(vec!["demo".into()]);
        std::fs::create_dir_all(root.join("workspace")).expect("workspace should be created");
        std::fs::create_dir_all(root.join("meta")).expect("meta should be created");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let run_id = format!("skill-spec-{}-{nanos}", std::process::id());
        let journal_path = root.join("meta/journal.jsonl");
        set_selected_bytes(&mut contract, "demo", None);
        let run = RunContext::for_tests(
            run_id.clone(),
            root.join("workspace"),
            root.join("meta"),
            contract.clone(),
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("test context should build");
        let journal = JournalWriter::create(&journal_path, &run_id, false, None)
            .expect("journal should open");
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let spec = skill_tool_spec(&spec_context(&run, &journal, &mut vars), &tool)
            .expect("unbounded skill should produce a catalog");
        assert!(spec.description.contains("demo"), "{}", spec.description);
        assert_eq!(
            spec.input_schema["properties"]["skill"]["enum"],
            json!(["demo"])
        );
        assert!(
            spec.description.contains("Demo skill."),
            "{}",
            spec.description
        );
        drop(journal);
        set_selected_bytes(&mut contract, "demo", Some(len - 1));
        let run = RunContext::for_tests(
            run_id.clone(),
            root.join("workspace"),
            root.join("meta"),
            contract.clone(),
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("bounded context should build");
        let journal = JournalWriter::create(&journal_path, &run_id, false, None)
            .expect("journal should reopen");
        let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
        let error = skill_tool_spec(&spec_context(&run, &journal, &mut vars), &tool)
            .expect_err("oversized SKILL.md must fail spec generation at the same bound");
        assert!(error.to_string().contains("max_bytes"), "{error}");
        drop(journal);
        std::fs::remove_dir_all(&root).expect("temporary directory should be removed");
    }
}
