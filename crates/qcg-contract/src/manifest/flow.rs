use qcg_policy::is_safe_relative_path;
use serde::Deserialize;
use std::collections::BTreeSet;

use super::assets::{validate_artifact_pattern, validate_tool};
use super::contract::{ContractError, stripped_error_message};
use super::nodes::{ContextRef, NodeDef};
use super::resources::CommandIsolation;
use super::validate::Manifest;

pub(crate) struct FlowNodeRule;

pub(crate) struct CommandPermissionRule;

impl CommandPermissionRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for command in &manifest.permissions.commands {
            let isolation = command.isolation.as_ref().ok_or_else(|| {
                ContractError::Invalid(format!(
                    "command permission `{}` must declare isolation as `container` or `trusted_host`",
                    command.bin
                ))
            })?;
            match isolation {
                CommandIsolation::Container => {
                    let image = command.image.as_deref().ok_or_else(|| {
                        ContractError::Invalid(format!(
                            "container-isolated command `{}` must declare image",
                            command.bin
                        ))
                    })?;
                    // The declared runtime selects the pin form; without one
                    // the strict digest rule applies.
                    let runtime = manifest.permissions.containers.runtime;
                    let pin_error = match runtime {
                        Some(runtime) => {
                            super::resources::validate_container_image(&runtime, image).err()
                        }
                        None => (!image.contains("@sha256:"))
                            .then(|| "image must be pinned by digest".to_string()),
                    };
                    if let Some(reason) = pin_error {
                        return Err(ContractError::Invalid(format!(
                            "container-isolated command `{}` image is not valid: {reason}",
                            command.bin
                        )));
                    }
                    if !manifest.permissions.containers.enabled
                        || !manifest
                            .permissions
                            .containers
                            .images
                            .iter()
                            .any(|allowed| allowed == image)
                    {
                        return Err(ContractError::Invalid(format!(
                            "container-isolated command `{}` image `{image}` must be allowed by permissions.containers",
                            command.bin
                        )));
                    }
                }
                CommandIsolation::TrustedHost if command.image.is_some() => {
                    return Err(ContractError::Invalid(format!(
                        "trusted-host command `{}` must not declare a container image",
                        command.bin
                    )));
                }
                CommandIsolation::TrustedHost => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ForeachValidationParams {
    max_iterations: Option<usize>,
    items: Option<String>,
    subflow: Option<String>,
    parallel: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct CheckToolValidationParams {
    tool: Option<String>,
}

impl FlowNodeRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut ids = BTreeSet::new();
        let mut errors = Vec::new();
        for node in &manifest.flow {
            if let Err(error) = self.validate_node(node, manifest, &mut ids) {
                errors.push(stripped_error_message(error));
            }
        }
        // Retry bounds bind block children too: the engine reads them for
        // every child attempt, so an out-of-range value must fail
        // validation rather than be silently corrected at runtime (E10).
        // Foreach value validation (items/subflow/parallel/range/product)
        // binds block-nested foreach nodes too, not just top-level flow.
        // Block-child `on_fail` is rejected at contract time (E10): repair
        // routing for block children is structurally infeasible — repair
        // targets are graph-scoped top-level nodes while block iterations
        // are namespaced per-iteration paths (`foreach[i].child`) with
        // iteration-local variable scopes and no `NodeState` slot; routing
        // them through the top-level repair cycle would alias repair state
        // across iterations and diverge journaling. Failing closed here
        // keeps block failures explicit (the foreach fails fast) instead of
        // silently ignoring the declared policy.
        for (block, nodes) in &manifest.blocks {
            for node in nodes {
                if let Err(message) = super::nodes::validate_node_id(&node.id) {
                    errors.push(stripped_error_message(ContractError::Invalid(format!(
                        "block `{block}` {message}"
                    ))));
                }
                if node.on_fail.is_some() {
                    errors.push(stripped_error_message(ContractError::Invalid(format!(
                        "block `{block}` node `{}` cannot declare on_fail; block-child repair routing is not supported, use top-level on_fail on the foreach node",
                        node.id
                    ))));
                }
                if let Err(error) = validate_retry_bounds(block, node) {
                    errors.push(stripped_error_message(error));
                }
                if node.kind.as_str() == "foreach"
                    && let Err(error) = validate_foreach_params(&node.id, node, manifest)
                {
                    errors.push(stripped_error_message(error));
                }
            }
        }
        match errors.len() {
            0 => Ok(()),
            1 => Err(ContractError::Invalid(errors.pop().unwrap_or_default())),
            count => Err(ContractError::Invalid(format!(
                "{count} invalid flow nodes:\n{}",
                errors
                    .iter()
                    .enumerate()
                    .map(|(index, error)| format!("{}. {error}", index + 1))
                    .collect::<Vec<_>>()
                    .join("\n")
            ))),
        }
    }

    fn validate_node(
        &self,
        node: &NodeDef,
        manifest: &Manifest,
        ids: &mut BTreeSet<String>,
    ) -> Result<(), ContractError> {
        if node.id.trim().is_empty() {
            return Err(ContractError::Invalid("flow node id is required".into()));
        }
        // Colon ambiguity (Q1): confirmation and operation ids join
        // `node_id:kind:digest:invocation` with `:` separators, so a node id
        // containing `:` would fork the 3/4-part parse. Reject explicitly.
        super::nodes::validate_node_id(&node.id).map_err(ContractError::Invalid)?;
        if !ids.insert(node.id.clone()) {
            return Err(ContractError::Invalid(format!(
                "duplicate flow node `{}`",
                node.id
            )));
        }
        if node.kind.as_str() == "foreach" {
            validate_foreach_params(&node.id, node, manifest)?;
        }
        if node.kind.as_str() == "check.tool" {
            let params: CheckToolValidationParams = node.deserialize_params().map_err(|error| {
                ContractError::Invalid(format!(
                    "check.tool node `{}` has invalid params: {error}",
                    node.id
                ))
            })?;
            let tool_name = params.tool.as_deref().ok_or_else(|| {
                ContractError::Invalid(format!("check.tool node `{}` must declare tool", node.id))
            })?;
            if !manifest.tools.contains_key(tool_name) {
                return Err(ContractError::Invalid(format!(
                    "check.tool node `{}` references unknown tool `{tool_name}`",
                    node.id
                )));
            }
        }
        if node.kind.is_llm()
            && !matches!(node.kind.as_str(), "llm.decide" | "llm.catalog")
            && manifest.llm.is_none()
        {
            return Err(ContractError::Invalid(format!(
                "node `{}` uses `{}` but [llm] is not declared",
                node.id, node.kind
            )));
        }
        for context in &node.context {
            validate_context_ref(&node.id, context, manifest)?;
        }
        validate_retry_bounds("flow", node)?;
        Ok(())
    }
}

/// Contract-time foreach value validation shared by flow nodes and
/// block-nested foreach nodes (E10). Runtime checks in the engine remain
/// as defense-in-depth, but empty items/subflow, out-of-range
/// parallel/max_iterations, unknown subflows, and retry-product storms
/// fail at build time.
fn validate_foreach_params(
    node_id: &str,
    node: &NodeDef,
    manifest: &Manifest,
) -> Result<(), ContractError> {
    let params: ForeachValidationParams = node.deserialize_params().map_err(|error| {
        ContractError::Invalid(format!(
            "foreach node `{node_id}` has invalid params: {error}"
        ))
    })?;
    let max_iterations = params.max_iterations.ok_or_else(|| {
        ContractError::Invalid(format!(
            "foreach node `{node_id}` must declare max_iterations"
        ))
    })?;
    let items = params.items.as_deref().unwrap_or("").trim();
    if items.is_empty() {
        return Err(ContractError::Invalid(format!(
            "foreach node `{node_id}` must declare a non-empty items expression"
        )));
    }
    let subflow = params.subflow.as_deref().unwrap_or("").trim();
    if subflow.is_empty() {
        return Err(ContractError::Invalid(format!(
            "foreach node `{node_id}` must declare a non-empty subflow"
        )));
    }
    let Some(block) = manifest.blocks.get(subflow) else {
        return Err(ContractError::Invalid(format!(
            "foreach node `{node_id}` references unknown subflow `{subflow}`"
        )));
    };
    if !(1..=qcg_policy::MAX_FOREACH_ITERATIONS).contains(&max_iterations) {
        return Err(ContractError::Invalid(format!(
            "foreach node `{node_id}` max_iterations must be from 1 through {}",
            qcg_policy::MAX_FOREACH_ITERATIONS
        )));
    }
    // No wire-compat default (Q1): `parallel` is required; omission fails
    // closed instead of silently running sequentially.
    let Some(parallel) = params.parallel else {
        return Err(ContractError::Invalid(format!(
            "foreach node `{node_id}` must declare `parallel`"
        )));
    };
    if !(1..=qcg_policy::MAX_FOREACH_PARALLELISM).contains(&parallel) {
        return Err(ContractError::Invalid(format!(
            "foreach node `{node_id}` parallel must be from 1 through {}",
            qcg_policy::MAX_FOREACH_PARALLELISM
        )));
    }
    let outer_max = node
        .retry
        .as_ref()
        .map(|retry| retry.max_attempts)
        .unwrap_or(1);
    for child in block {
        let child_max = child
            .retry
            .as_ref()
            .map(|retry| retry.max_attempts)
            .unwrap_or(1);
        let product = (outer_max as usize).saturating_mul(child_max as usize);
        if product > qcg_policy::MAX_RETRY_ATTEMPTS as usize {
            return Err(ContractError::Invalid(format!(
                "foreach node `{node_id}` retry product {product} (outer {outer_max} * child `{}` {child_max}) exceeds {}",
                child.id,
                qcg_policy::MAX_RETRY_ATTEMPTS
            )));
        }
        // Recursive total-product across nesting depth (E10): a block child
        // that is itself a foreach multiplies again. The total along any
        // nesting path must stay within the cap, not just the direct
        // outer*child product.
        if child.kind.as_str() == "foreach" {
            let mut visited = std::collections::BTreeSet::new();
            visited.insert(subflow.to_string());
            let nested_total =
                foreach_total_product(child, manifest, &mut visited).map_err(|message| {
                    ContractError::Invalid(format!(
                        "foreach node `{node_id}` nested retry product exceeds {} via child `{}`: {message}",
                        qcg_policy::MAX_RETRY_ATTEMPTS,
                        child.id
                    ))
                })?;
            let total = (outer_max as usize).saturating_mul(nested_total);
            if total > qcg_policy::MAX_RETRY_ATTEMPTS as usize {
                return Err(ContractError::Invalid(format!(
                    "foreach node `{node_id}` total retry product {total} (outer {outer_max} * nested {nested_total} via child `{}`) exceeds {}",
                    child.id,
                    qcg_policy::MAX_RETRY_ATTEMPTS
                )));
            }
        }
    }
    Ok(())
}

/// Recursive total attempt product for a foreach subtree (E10). Returns the
/// maximum product along any nesting path starting at `node`, including its
/// own outer attempts and all nested foreach levels. `visited` guards block
/// cycles fail-closed. The cap check itself lives in the caller so both
/// direct and nested violations share one message shape.
fn foreach_total_product(
    node: &NodeDef,
    manifest: &Manifest,
    visited: &mut std::collections::BTreeSet<String>,
) -> Result<usize, String> {
    let params: ForeachValidationParams = node
        .deserialize_params()
        .map_err(|error| format!("invalid nested foreach params: {error}"))?;
    let subflow = params.subflow.as_deref().unwrap_or("").trim();
    if subflow.is_empty() {
        return Err("nested foreach declares an empty subflow".into());
    }
    if !visited.insert(subflow.to_string()) {
        return Err(format!("block cycle detected at `{subflow}`"));
    }
    let Some(block) = manifest.blocks.get(subflow) else {
        return Err(format!("unknown nested subflow `{subflow}`"));
    };
    let outer_max = node
        .retry
        .as_ref()
        .map(|retry| retry.max_attempts)
        .unwrap_or(1) as usize;
    let mut worst = outer_max;
    for child in block {
        let child_max = child
            .retry
            .as_ref()
            .map(|retry| retry.max_attempts)
            .unwrap_or(1) as usize;
        let direct = outer_max.saturating_mul(child_max);
        if direct > worst {
            worst = direct;
        }
        if child.kind.as_str() == "foreach" {
            let nested = foreach_total_product(child, manifest, visited)?;
            let total = outer_max.saturating_mul(nested);
            if total > worst {
                worst = total;
            }
        }
    }
    visited.remove(subflow);
    Ok(worst)
}

/// Retry bounds shared by flow nodes and foreach block children: the
/// engine reads these values for every attempt, so validation (not a
/// runtime `max(1)` correction) is where out-of-range values fail (E10).
fn validate_retry_bounds(scope: &str, node: &NodeDef) -> Result<(), ContractError> {
    let location = if scope == "flow" {
        format!("node `{}`", node.id)
    } else {
        format!("block `{scope}` node `{}`", node.id)
    };
    if let Some(retry) = &node.retry {
        if retry.max_attempts == 0 || retry.max_attempts > qcg_policy::MAX_RETRY_ATTEMPTS {
            return Err(ContractError::Invalid(format!(
                "{location} retry.max_attempts must be between 1 and {}",
                qcg_policy::MAX_RETRY_ATTEMPTS,
            )));
        }
        if retry.backoff_ms > qcg_policy::MAX_RETRY_BACKOFF_MS {
            return Err(ContractError::Invalid(format!(
                "{location} retry.backoff_ms must not exceed {}",
                qcg_policy::MAX_RETRY_BACKOFF_MS,
            )));
        }
        if retry.timeout_secs.is_some_and(|timeout| timeout == 0) {
            return Err(ContractError::Invalid(format!(
                "{location} retry.timeout_secs must be at least 1 when set",
            )));
        }
    }
    Ok(())
}

fn validate_context_ref(
    node_id: &str,
    context: &ContextRef,
    manifest: &Manifest,
) -> Result<(), ContractError> {
    let ContextRef::Resource(reference) = context else {
        if let ContextRef::Short(reference) = context {
            if reference == "inputs.*"
                || reference.starts_with("inputs.")
                || reference.starts_with("steps.")
            {
                return Ok(());
            }
            if let Some(resource) = reference.strip_prefix("resources.") {
                let name = resource.split_once('#').map_or(resource, |(name, _)| name);
                if manifest.resources.contains_key(name) {
                    return Ok(());
                }
                return Err(ContractError::Invalid(format!(
                    "node `{node_id}` context references unknown resource `{name}`"
                )));
            }
            return Err(ContractError::Invalid(format!(
                "node `{node_id}` has unsupported context reference `{reference}`"
            )));
        }
        return Ok(());
    };
    let resource = manifest.resources.get(&reference.resource).ok_or_else(|| {
        ContractError::Invalid(format!(
            "node `{node_id}` context references unknown resource `{}`",
            reference.resource
        ))
    })?;
    let select = reference.select.as_deref();
    if select.is_none() && (reference.tag.is_some() || reference.path.is_some()) {
        return Err(ContractError::Invalid(format!(
            "node `{node_id}` resource context tag/path requires select"
        )));
    }
    match resource.kind.as_str() {
        "openapi" => match select {
            None if reference.tag.is_none() && reference.path.is_none() => Ok(()),
            Some("paths") if reference.tag.is_none() && reference.path.is_none() => Ok(()),
            Some("operations") if reference.path.is_none() => Ok(()),
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid OpenAPI selector for resource `{}`",
                reference.resource
            ))),
        },
        "skill" => match select {
            None | Some("instructions" | "meta" | "tree" | "files")
                if reference.tag.is_none() && reference.path.is_none() =>
            {
                Ok(())
            }
            Some("file" | "files")
                if reference.tag.is_none()
                    && reference.path.as_deref().is_some_and(is_safe_relative_path) =>
            {
                Ok(())
            }
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid skill selector for resource `{}`",
                reference.resource
            ))),
        },
        "skill_library" => match select {
            None | Some("catalog" | "tree" | "files")
                if reference.tag.is_none() && reference.path.is_none() =>
            {
                Ok(())
            }
            Some("meta" | "instructions" | "tree")
                if reference.tag.is_none()
                    && reference.path.as_deref().is_some_and(is_safe_relative_path) =>
            {
                Ok(())
            }
            Some("file" | "files")
                if reference.tag.is_none()
                    && reference
                        .path
                        .as_deref()
                        .is_some_and(|path| is_safe_relative_path(path) && path.contains('/')) =>
            {
                Ok(())
            }
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid skill_library selector for resource `{}`",
                reference.resource
            ))),
        },
        "dir" => match select {
            None | Some("tree" | "files")
                if reference.tag.is_none() && reference.path.is_none() =>
            {
                Ok(())
            }
            Some("file")
                if reference.tag.is_none()
                    && reference.path.as_deref().is_some_and(is_safe_relative_path) =>
            {
                Ok(())
            }
            _ => Err(ContractError::Invalid(format!(
                "node `{node_id}` has invalid directory selector for resource `{}`",
                reference.resource
            ))),
        },
        _ if select.is_none() && reference.tag.is_none() && reference.path.is_none() => Ok(()),
        _ => Err(ContractError::Invalid(format!(
            "node `{node_id}` resource `{}` does not support selectors",
            reference.resource
        ))),
    }
}

pub(crate) struct ToolRule;

impl ToolRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        for (name, tool) in &manifest.tools {
            validate_tool(name, tool, &manifest.permissions)?;
        }
        Ok(())
    }
}

pub(crate) struct OutputArtifactRule;

impl OutputArtifactRule {
    pub(crate) fn validate(&self, manifest: &Manifest) -> Result<(), ContractError> {
        let mut declared = BTreeSet::new();
        if let Some((block, node)) = manifest
            .blocks
            .iter()
            .flat_map(|(block, nodes)| {
                nodes
                    .iter()
                    .filter(|node| node.artifact.is_some())
                    .map(move |node| (block, node))
            })
            .next()
        {
            return Err(ContractError::Invalid(format!(
                "block `{block}` node `{}` cannot declare a top-level artifact",
                node.id
            )));
        }
        for node in manifest.flow.iter().filter(|node| node.artifact.is_some()) {
            let artifact = node.artifact.as_ref().ok_or_else(|| {
                ContractError::Invalid(format!("node `{}` lost its artifact declaration", node.id))
            })?;
            validate_artifact_mime(artifact.mime.as_deref(), &format!("node `{}`", node.id))?;
            let Some(path) = node.artifact_path_template() else {
                return Err(ContractError::Invalid(format!(
                    "node `{}` declares artifact metadata but its step has no static output_file, target, or destination parameter",
                    node.id
                )));
            };
            validate_artifact_pattern(path, "artifact path")?;
            if !declared.insert(path.to_string()) {
                return Err(ContractError::Invalid(format!(
                    "artifact path `{path}` is declared by more than one node"
                )));
            }
        }
        for extra in &manifest.outputs.extras {
            validate_artifact_pattern(&extra.glob, "output glob")?;
            validate_artifact_mime(extra.mime.as_deref(), &format!("glob `{}`", extra.glob))?;
        }
        Ok(())
    }
}

fn validate_artifact_mime(mime: Option<&str>, declaration: &str) -> Result<(), ContractError> {
    let Some(mime) = mime else {
        return Ok(());
    };
    mime.parse::<mime::Mime>().map_err(|error| {
        ContractError::Invalid(format!(
            "artifact {declaration} has invalid MIME type `{mime}`: {error}"
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod retry_bounds_tests {
    use super::*;
    use crate::manifest::Manifest;

    fn manifest_with_block_retry(max_attempts: u32) -> Manifest {
        toml::from_str::<Manifest>(&format!(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "loop"
type = "foreach"
[flow.params]
items = "inputs.items"
subflow = "item"
max_iterations = 4
parallel = 1
[flow.retry]
max_attempts = 2
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"

[[blocks.item]]
id = "child"
type = "write"
[blocks.item.retry]
max_attempts = {max_attempts}
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"
"#
        ))
        .expect("manifest should parse")
    }

    #[test]
    fn block_children_share_flow_retry_bounds() {
        // E10: out-of-range retry policy in a block child fails validation
        // instead of being silently corrected at runtime.
        let error = FlowNodeRule
            .validate(&manifest_with_block_retry(0))
            .expect_err("max_attempts = 0 in a block must be refused");
        assert!(
            error.to_string().contains("must be between 1 and 16"),
            "refusal must name the bound: {error}"
        );
        FlowNodeRule
            .validate(&manifest_with_block_retry(3))
            .expect("in-range block retry must validate");
    }

    fn manifest_with_foreach(
        items: Option<&str>,
        subflow: Option<&str>,
        max_iterations: i64,
        parallel: i64,
    ) -> Manifest {
        let items_line = items
            .map(|value| format!("items = \"{value}\""))
            .unwrap_or_default();
        let subflow_line = subflow
            .map(|value| format!("subflow = \"{value}\""))
            .unwrap_or_default();
        toml::from_str::<Manifest>(&format!(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "loop"
type = "foreach"
[flow.params]
{items_line}
{subflow_line}
max_iterations = {max_iterations}
parallel = {parallel}

[[blocks.item]]
id = "child"
type = "write"
"#
        ))
        .expect("manifest should parse")
    }

    #[test]
    fn foreach_contract_time_rejects_empty_and_out_of_range() {
        // E10: items/subflow-empty and parallel/range violations fail at
        // contract build time, not at execution.
        let error = FlowNodeRule
            .validate(&manifest_with_foreach(None, Some("item"), 4, 1))
            .expect_err("empty items must be refused at contract time");
        assert!(error.to_string().contains("non-empty items"), "{error}");
        let error = FlowNodeRule
            .validate(&manifest_with_foreach(
                Some("inputs.items"),
                Some("missing"),
                4,
                1,
            ))
            .expect_err("unknown subflow must be refused at contract time");
        assert!(error.to_string().contains("unknown subflow"), "{error}");
        let error = FlowNodeRule
            .validate(&manifest_with_foreach(
                Some("inputs.items"),
                Some("item"),
                4,
                0,
            ))
            .expect_err("parallel = 0 must be refused at contract time");
        assert!(error.to_string().contains("parallel"), "{error}");
    }

    #[test]
    fn foreach_retry_product_cap_rejects_storms() {
        // E10: outer max_attempts * child max_attempts beyond 16 fails at
        // contract time even when each bound is individually valid.
        let mut manifest = manifest_with_block_retry(8);
        // Outer is 2 (see helper), child 8 => product 16 passes.
        FlowNodeRule
            .validate(&manifest)
            .expect("product 16 must validate");
        manifest = manifest_with_block_retry(9);
        let error = FlowNodeRule
            .validate(&manifest)
            .expect_err("product 18 must be refused");
        assert!(error.to_string().contains("retry product"), "{error}");
    }

    #[test]
    fn node_ids_reject_colons_for_confirmation_unambiguity() {
        // Q1: `node_id:kind:digest:invocation` joins with `:`, so a colon in
        // the id would fork 3/4-part confirmation parsing. Both flow and
        // block ids fail closed with an explicit message.
        let manifest: Manifest = toml::from_str(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "bad:id"
type = "write"
"#,
        )
        .expect("manifest should parse");
        let error = FlowNodeRule
            .validate(&manifest)
            .expect_err("colon in flow id must be refused");
        assert!(
            error.to_string().contains("must not contain `:`"),
            "{error}"
        );
        let manifest: Manifest = toml::from_str(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "loop"
type = "foreach"
[flow.params]
items = "inputs.items"
subflow = "item"
max_iterations = 2

[[blocks.item]]
id = "bad:child"
type = "write"
"#,
        )
        .expect("manifest should parse");
        let error = FlowNodeRule
            .validate(&manifest)
            .expect_err("colon in block id must be refused");
        assert!(
            error.to_string().contains("must not contain `:`"),
            "{error}"
        );
    }

    #[test]
    fn block_children_reject_on_fail_routing() {
        // E10: block-child `on_fail` is structurally infeasible (repair
        // targets are graph-scoped while iterations are namespaced), so it
        // fails closed at contract time instead of being silently ignored.
        let manifest: Manifest = toml::from_str(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "loop"
type = "foreach"
[flow.params]
items = "inputs.items"
subflow = "item"
max_iterations = 2

[[blocks.item]]
id = "child"
type = "write"
[blocks.item.on_fail]
action = "fail"
"#,
        )
        .expect("manifest should parse");
        let error = FlowNodeRule
            .validate(&manifest)
            .expect_err("block on_fail must be refused");
        assert!(
            error.to_string().contains("cannot declare on_fail"),
            "refusal must name the cause: {error}"
        );
    }

    #[test]
    fn nested_foreach_total_product_rejects_three_layer_storms() {
        // E10: recursive total-product across nesting depth. Outer 2 *
        // middle 2 * inner 5 = 20 exceeds 16 even though each direct
        // outer*child product (2*2=4, 2*5=10) passes individually.
        let manifest: Manifest = toml::from_str(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "outer"
type = "foreach"
[flow.params]
items = "inputs.items"
subflow = "middle"
max_iterations = 2
parallel = 1
[flow.retry]
max_attempts = 2
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"

[[blocks.middle]]
id = "mid"
type = "foreach"
[blocks.middle.params]
items = "inputs.items"
subflow = "inner"
max_iterations = 2
parallel = 1
[blocks.middle.retry]
max_attempts = 2
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"

[[blocks.inner]]
id = "leaf"
type = "write"
[blocks.inner.retry]
max_attempts = 5
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"
"#,
        )
        .expect("manifest should parse");
        let error = FlowNodeRule
            .validate(&manifest)
            .expect_err("three-layer product 20 must be refused");
        assert!(
            error.to_string().contains("total retry product"),
            "refusal must name the total product: {error}"
        );
        // Passing shape: 2 * 2 * 4 = 16 validates.
        let manifest: Manifest = toml::from_str(
            r#"
[generator]
id = "x"
name = "X"
version = "0.1.0"
qcg_version = "^0.1"
description = "test"

[[flow]]
id = "outer"
type = "foreach"
[flow.params]
items = "inputs.items"
subflow = "middle"
max_iterations = 2
parallel = 1
[flow.retry]
max_attempts = 2
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"

[[blocks.middle]]
id = "mid"
type = "foreach"
[blocks.middle.params]
items = "inputs.items"
subflow = "inner"
max_iterations = 2
parallel = 1
[blocks.middle.retry]
max_attempts = 2
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"

[[blocks.inner]]
id = "leaf"
type = "write"
[blocks.inner.retry]
max_attempts = 4
backoff_ms = 0
timeout_secs = 30
on_indeterminate = "fail"
"#,
        )
        .expect("manifest should parse");
        FlowNodeRule
            .validate(&manifest)
            .expect("three-layer product 16 must validate");
    }
}
