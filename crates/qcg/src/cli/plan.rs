use anyhow::{Context, Result};
use qcg_contract::Contract;
use qcg_policy::MAX_CONFIRM_INPUT_BYTES;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead as _, Read as _, Write};

pub(crate) fn print_run_plan(
    contract: &Contract,
    inputs: &BTreeMap<String, Value>,
    answers: &BTreeMap<String, Value>,
    confirmations: &BTreeMap<String, bool>,
    json_output: bool,
    show_diff: bool,
) -> Result<()> {
    let manifest = &contract.manifest;
    let mut missing_answers = Vec::new();
    let mut steps = Vec::new();
    for node in &manifest.flow {
        let kind = node.kind.as_str();
        if kind == "ask_user" && !answers.contains_key(&node.id) {
            missing_answers.push(node.id.clone());
        }
        let mut entry = json!({
            "id": node.id,
            "type": kind,
            "needs": node.needs,
            "when": node.when.as_ref().map(|when| when.0.clone()),
            "artifact": node.artifact.as_ref().map(|artifact| artifact.label.clone()),
            "pre_provisioned_answer": answers.contains_key(&node.id),
            "retry": node.retry.as_ref().map(|retry| json!({
                "max_attempts": retry.max_attempts,
                "backoff_ms": retry.backoff_ms,
                "timeout_secs": retry.timeout_secs,
            })),
        });
        if show_diff && let Value::Object(map) = &mut entry {
            map.insert("forecast".into(), forecast_node(contract, node));
        }
        steps.push(entry);
    }
    let mut plan = json!({
        "generator": format!("{}@{}", manifest.generator.id, manifest.generator.version),
        "contract_sha256": contract.sha256,
        "inputs": inputs,
        "steps": steps,
        "parallel": manifest.parallel,
        "permissions": {
            "fs_read": manifest.permissions.fs_read,
            "fs_write": manifest.permissions.fs_write,
            "network": manifest.permissions.network,
            "commands": manifest.permissions.commands.iter().map(|command| json!({
                "bin": command.bin,
                "args": command.args,
                "isolation": format!("{:?}", command.isolation),
                "image": command.image,
            })).collect::<Vec<_>>(),
            "side_effects": format!("{:?}", manifest.permissions.side_effects),
        },
        "outputs": manifest.outputs.extras.iter().map(|extra| json!({
            "glob": extra.glob,
            "label": extra.label,
            "required": extra.required,
        })).collect::<Vec<_>>(),
        "pre_provisioned": {
            "answers": answers.keys().collect::<Vec<_>>(),
            "confirmations": confirmations.keys().collect::<Vec<_>>(),
            "missing_answers": missing_answers,
        },
    });
    if show_diff && let Value::Object(map) = &mut plan {
        map.insert("estimates".into(), estimate_plan(contract));
    }
    if json_output {
        println!("{}", serde_json::to_string_pretty(&plan)?);
        return Ok(());
    }
    println!(
        "plan: {}@{} ({})",
        manifest.generator.id, manifest.generator.version, contract.sha256
    );
    println!("steps:");
    for node in &manifest.flow {
        let kind = node.kind.as_str();
        let mut markers = Vec::new();
        if !node.needs.is_empty() {
            markers.push(format!("needs={}", node.needs.join(",")));
        }
        if node.when.is_some() {
            markers.push("when=...".to_string());
        }
        if node.artifact.is_some() {
            markers.push("artifact".to_string());
        }
        if kind == "ask_user" {
            markers.push(if answers.contains_key(&node.id) {
                "answer=provided".to_string()
            } else {
                "answer=MISSING".to_string()
            });
        }
        println!(
            "  - {} [{}]{}",
            node.id,
            kind,
            if markers.is_empty() {
                String::new()
            } else {
                format!(" ({})", markers.join(", "))
            }
        );
    }
    if manifest.outputs.extras.is_empty() {
        println!("outputs: none declared (workspace side effects only)");
    } else {
        println!("outputs:");
        for extra in &manifest.outputs.extras {
            println!(
                "  - {} (glob={}, required={})",
                extra.label, extra.glob, extra.required
            );
        }
    }
    println!(
        "side_effects policy: {:?}; pre-provisioned confirmations: {}",
        manifest.permissions.side_effects,
        confirmations.len()
    );
    if !missing_answers.is_empty() {
        println!(
            "warning: missing pre-provisioned answers for: {}",
            missing_answers.join(", ")
        );
    }
    if show_diff {
        println!("forecast (read-only, no execution):");
        for node in &manifest.flow {
            let forecast = forecast_node(contract, node);
            println!(
                "  - {}: writes={} network={} command={} side_effect={}",
                node.id,
                forecast
                    .get("writes")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
                forecast
                    .get("network_host")
                    .and_then(Value::as_str)
                    .unwrap_or("-"),
                forecast
                    .get("command_allowed")
                    .map_or("n/a".to_string(), |value| value.to_string()),
                forecast
                    .get("side_effect")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            );
            for warning in forecast
                .get("warnings")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(text) = warning.as_str() {
                    println!("      ! {text}");
                }
            }
        }
        println!(
            "estimates: {}",
            serde_json::to_string(&estimate_plan(contract))?
        );
    }
    Ok(())
}

/// Read-only forecast for one node: no filesystem writes, no processes, no network.
/// All checks mirror the permission rules used at execution time.
fn forecast_node(contract: &Contract, node: &qcg_contract::NodeDef) -> Value {
    let manifest = &contract.manifest;
    let params = node.params_json();
    let mut warnings = Vec::new();
    let mut writes: Vec<Value> = Vec::new();
    for key in ["output_file", "target", "destination"] {
        if let Some(path) = params.get(key).and_then(Value::as_str) {
            writes.push(Value::String(path.to_string()));
        }
    }
    // Workspace write forecast: any declared file output needs fs_write workspace.
    let can_write = manifest
        .permissions
        .fs_write
        .iter()
        .any(|scope| scope == "workspace");
    if !writes.is_empty() && !can_write {
        warnings.push(Value::String(
            "workspace write denied by permissions.fs_write".to_string(),
        ));
    }
    // Command forecast: exact bin plus "*" wildcard args, same as the gateway.
    let mut command_allowed = None;
    if let Some(command) = params.get("command").and_then(Value::as_array) {
        let argv = command
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        command_allowed = Some(plan_command_allowed(manifest, &argv));
        if command_allowed == Some(false) {
            warnings.push(Value::String(
                "command is not declared in permissions.commands".to_string(),
            ));
        }
    }
    // Network forecast for http nodes.
    let mut network_host = None;
    let mut network_allowed = None;
    if node.kind.as_str() == "http"
        && let Some(url) = params.get("url").and_then(Value::as_str)
    {
        // Templates are not rendered here; only literal hosts are forecast.
        if url.contains("{{") {
            warnings.push(Value::String(
                "http url contains a template; host check deferred to execution".to_string(),
            ));
        } else if let Ok(parsed) = url::Url::parse(url) {
            let host = parsed.host_str().unwrap_or("").to_string();
            network_host = Some(host.clone());
            let allowed = manifest.permissions.network.iter().any(|entry| {
                entry == "*"
                    || entry == &host
                    || url::Url::parse(entry)
                        .ok()
                        .and_then(|value| value.host_str().map(str::to_string))
                        .as_deref()
                        == Some(host.as_str())
            });
            network_allowed = Some(allowed);
            if !allowed {
                warnings.push(Value::String(format!(
                    "network host `{host}` is not allowed by permissions.network"
                )));
            }
        } else {
            warnings.push(Value::String("http url is not parseable".to_string()));
        }
    }
    let side_effect = matches!(node.kind.as_str(), "http" | "command" | "mcp.call")
        || params
            .get("side_effects")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    if side_effect && !confirmations_covered(manifest, node) {
        // Confirmation coverage is checked by id at execution; here we only warn
        // when the generator requires confirmation for every side effect.
        if matches!(
            manifest.permissions.side_effects,
            qcg_contract::SideEffects::Confirm
        ) {
            warnings.push(Value::String(
                "side_effects=confirm requires pre-provisioned confirmation at execution"
                    .to_string(),
            ));
        }
    }
    json!({
        "writes": writes,
        "command_allowed": command_allowed,
        "network_host": network_host,
        "network_allowed": network_allowed,
        "side_effect": side_effect,
        "warnings": warnings,
    })
}

fn confirmations_covered(
    _manifest: &qcg_contract::Manifest,
    _node: &qcg_contract::NodeDef,
) -> bool {
    // Coverage depends on runtime confirmation ids; the read-only forecast
    // must not claim coverage it cannot prove.
    false
}

pub(crate) fn plan_command_allowed(manifest: &qcg_contract::Manifest, argv: &[String]) -> bool {
    let Some((bin, args)) = argv.split_first() else {
        return false;
    };
    manifest
        .permissions
        .commands
        .iter()
        .filter(|permission| permission.bin == *bin)
        .any(|permission| {
            if permission.args.is_empty() {
                return args.is_empty();
            }
            if permission.args.len() != args.len() {
                return false;
            }
            permission
                .args
                .iter()
                .zip(args)
                .all(|(pattern, actual)| pattern == "*" || pattern == actual)
        })
}

/// Whole-plan estimates from declared budgets only.
fn estimate_plan(contract: &Contract) -> Value {
    let manifest = &contract.manifest;
    let llm_nodes = manifest
        .flow
        .iter()
        .filter(|node| node.kind.as_str().starts_with("llm."))
        .count();
    json!({
        "steps": manifest.flow.len(),
        "llm_nodes": llm_nodes,
        "max_steps": manifest.budget.max_steps,
        "max_tokens": manifest.budget.max_tokens,
        "max_elapsed_seconds": manifest.budget.max_elapsed_seconds,
        "max_cost_usd": manifest.budget.max_cost_usd,
        "command_timeout_seconds": manifest.runtime.command_timeout_seconds,
        "http_timeout_seconds": manifest.runtime.http_timeout_seconds,
    })
}

pub(crate) fn print_permission_summary(contract: &Contract) {
    let permissions = &contract.manifest.permissions;
    println!(
        "generator: {}@{}",
        contract.manifest.generator.id, contract.manifest.generator.version
    );
    println!("permissions:");
    println!("  fs_read: {}", join_or_none(&permissions.fs_read));
    println!("  fs_write: {}", join_or_none(&permissions.fs_write));
    println!("  network: {}", join_or_none(&permissions.network));
    println!(
        "  commands: {}",
        if permissions.commands.is_empty() {
            "none".into()
        } else {
            permissions
                .commands
                .iter()
                .map(|command| {
                    format!(
                        "{} {:?} isolation={:?} image={}",
                        command.bin,
                        command.args,
                        command.isolation,
                        command.image.as_deref().unwrap_or("none")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    println!(
        "  containers: enabled={}, runtime={}, images={}, on_missing={}",
        permissions.containers.enabled,
        permissions
            .containers
            .runtime
            .map(|runtime| format!("{runtime:?}"))
            .unwrap_or_else(|| "none".into()),
        join_or_none(&permissions.containers.images),
        permissions
            .containers
            .on_missing
            .as_deref()
            .unwrap_or("error")
    );
    println!("  side_effects: {:?}", permissions.side_effects);
    println!(
        "  secrets: {}",
        if contract.manifest.secrets.is_empty() {
            "none".into()
        } else {
            contract
                .manifest
                .secrets
                .iter()
                .map(|(name, secret)| {
                    format!(
                        "{name} ({})",
                        secret.source_label().unwrap_or_else(|| "invalid".into())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
}

fn join_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}

pub(crate) fn confirm_stdin(prompt: &str) -> Result<()> {
    eprint!("{prompt} [y/N] ");
    std::io::stderr().flush()?;
    let answer = read_bounded_confirmation(&mut std::io::stdin().lock())?;
    if matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        Ok(())
    } else {
        anyhow::bail!("operation cancelled")
    }
}

pub(crate) fn read_bounded_confirmation<R: std::io::BufRead>(reader: &mut R) -> Result<String> {
    let mut bytes = Vec::with_capacity(MAX_CONFIRM_INPUT_BYTES);
    reader
        .take((MAX_CONFIRM_INPUT_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.len() > MAX_CONFIRM_INPUT_BYTES {
        anyhow::bail!("confirmation input exceeds {MAX_CONFIRM_INPUT_BYTES} bytes");
    }
    String::from_utf8(bytes).context("confirmation input must be valid UTF-8")
}

pub(crate) fn ensure_safe_install_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.starts_with('/')
        || id.contains('\\')
        || id.contains('\0')
        || id.split('/').any(|part| part == ".." || part.is_empty())
    {
        anyhow::bail!("generator id `{id}` is not safe for installation");
    }
    Ok(())
}
