use qcg_contract::{
    Contract, NodeDef, ResourceKind, ToolDecl, is_skill_library_entry, parse_skill_doc,
    validate_skill_doc,
};
use qcg_engine::{ResourceSelector, ResultExt, StepContext, StepError, select_resource};
use qcg_llm::ToolSpec;
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
        match resource.kind {
            ResourceKind::Skill => {
                let path = resource
                    .path
                    .as_deref()
                    .ok_or_else(|| format!("skill resource `{resource_name}` requires path"))?;
                let root = contract.resolve_package_path(path).map_err(|error| {
                    format!("skill resource `{resource_name}` path is invalid: {error}")
                })?;
                let doc = read_skill_doc(resource_name, &root)?;
                bindings.push(SkillBinding {
                    name: doc.name,
                    resource: resource_name.clone(),
                    library_dir: None,
                });
            }
            ResourceKind::SkillLibrary => {
                let path = resource.path.as_deref().ok_or_else(|| {
                    format!("skill_library resource `{resource_name}` requires path")
                })?;
                let root = contract.resolve_package_path(path).map_err(|error| {
                    format!("skill_library resource `{resource_name}` path is invalid: {error}")
                })?;
                for dir in library_skill_dirs(resource_name, &root)? {
                    read_skill_doc(resource_name, &root.join(&dir))?;
                    bindings.push(SkillBinding {
                        name: dir.clone(),
                        resource: resource_name.clone(),
                        library_dir: Some(dir),
                    });
                }
            }
            _ => {
                return Err(format!(
                    "skill tool resource `{resource_name}` must use type `skill` or `skill_library`"
                ));
            }
        }
    }
    Ok(bindings)
}

fn read_skill_doc(
    resource: &str,
    root: &camino::Utf8Path,
) -> Result<qcg_contract::SkillDoc, String> {
    let skill_path = root.join("SKILL.md");
    let source = std::fs::read_to_string(&skill_path).map_err(|error| {
        format!("skill resource `{resource}` cannot read `{skill_path}`: {error}")
    })?;
    let doc = parse_skill_doc(&source)
        .map_err(|error| format!("skill resource `{resource}` has invalid SKILL.md: {error}"))?;
    validate_skill_doc(&doc, root.file_name())
        .map_err(|error| format!("skill resource `{resource}` is invalid: {error}"))?;
    Ok(doc)
}

fn library_skill_dirs(resource: &str, root: &camino::Utf8Path) -> Result<Vec<String>, String> {
    let entries = std::fs::read_dir(root).map_err(|error| {
        format!("skill_library resource `{resource}` cannot read `{root}`: {error}")
    })?;
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!("skill_library resource `{resource}` cannot read entry: {error}")
        })?;
        let file_type = entry.file_type().map_err(|error| {
            format!("skill_library resource `{resource}` cannot inspect entry: {error}")
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !is_skill_library_entry(&name) {
            continue;
        }
        if root.join(&name).join("SKILL.md").is_file() {
            dirs.push(name);
        }
    }
    dirs.sort();
    Ok(dirs)
}
