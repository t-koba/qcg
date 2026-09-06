use crate::summaries::{read_events_from_meta, run_meta_dir, run_workspace_dir};
use crate::types::{RunRecord, ServiceError};
use camino::{Utf8Path, Utf8PathBuf};
use fs2::FileExt as _;
use qcg_api::ForkStatePatch;
use qcg_engine::{JournalLimits, JournalWriter, RunState};
use qcg_policy::is_safe_relative_path;
use qcg_steps::deterministic_registry;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::sync::Arc;

pub(crate) fn write_run_event(
    record: &RunRecord,
    kind: &str,
    payload: Value,
) -> Result<(), ServiceError> {
    let run_id = record
        .run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?;
    JournalWriter::create_with_limits(
        &run_meta_dir(&record.run_dir).join("journal.jsonl"),
        run_id,
        false,
        Some(record.events.clone()),
        JournalLimits::from(&record.contract.manifest.runtime),
    )
    .map_err(|error| ServiceError::Invalid(error.to_string()))?
    .event(kind, payload)
    .map_err(|error| ServiceError::Invalid(error.to_string()))
}

pub(crate) fn direct_run_id(workspace: &Utf8Path) -> String {
    let digest = hex::encode(Sha256::digest(workspace.as_str().as_bytes()));
    format!("direct-{digest}")
}

fn is_lock_contention(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }

    #[cfg(windows)]
    {
        // LockFileEx reports sharing and lock violations without mapping them to WouldBlock.
        matches!(error.raw_os_error(), Some(32 | 33))
    }

    #[cfg(not(windows))]
    {
        false
    }
}

pub(crate) fn lock_runs_directory(runs_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let lock_path = runs_dir.join(".service.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    file.try_lock_exclusive().map_err(|error| {
        if is_lock_contention(&error) {
            ServiceError::Invalid(format!(
                "runs directory `{runs_dir}` is already owned by another qcg service"
            ))
        } else {
            ServiceError::Io(error)
        }
    })?;
    Ok(file)
}

pub(crate) fn try_lock_run_execution(run_dir: &Utf8Path) -> Result<Option<File>, ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(metadata.join("execution.lock"))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if is_lock_contention(&error) => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

pub(crate) fn try_lock_store_maintenance(
    runs_dir: &Utf8Path,
) -> Result<Option<File>, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(runs_dir.join(".maintenance.lock"))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if is_lock_contention(&error) => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

pub(crate) fn prepare_api_run_directory(run_dir: &Utf8Path) -> Result<(), ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(metadata.join("journal.jsonl"))?;
    Ok(())
}

pub(crate) fn prepare_checkpoint_fork(
    source_dir: &Utf8Path,
    source_id: &str,
    target_dir: &Utf8Path,
    target_id: &str,
    at_seq: u64,
    patch: &ForkStatePatch,
) -> Result<(), ServiceError> {
    let source_meta = run_meta_dir(source_dir);
    let source_events = read_events_from_meta(&source_meta)?;
    if !source_events
        .iter()
        .any(|event| event.get("seq").and_then(Value::as_u64) == Some(at_seq))
    {
        return Err(ServiceError::Invalid(format!(
            "run `{source_id}` has no checkpoint sequence {at_seq}"
        )));
    }
    let selected = source_events
        .into_iter()
        .filter(|event| {
            event
                .get("seq")
                .and_then(Value::as_u64)
                .is_some_and(|seq| seq <= at_seq)
        })
        .filter(|event| {
            !matches!(
                event.get("t").and_then(Value::as_str),
                Some(
                    "run_finished"
                        | "run_error"
                        | "run_canceled"
                        | "run_interrupted"
                        | "run_waiting"
                        | "confirm_request"
                )
            )
        })
        .collect::<Vec<_>>();
    if !selected.iter().any(|event| {
        matches!(
            event.get("t").and_then(Value::as_str),
            Some("run_started" | "run_queued")
        )
    }) {
        return Err(ServiceError::Invalid(format!(
            "checkpoint {source_id}@{at_seq} predates run initialization"
        )));
    }

    prepare_api_run_directory(target_dir)?;
    let target_meta = run_meta_dir(target_dir);
    let target_workspace = run_workspace_dir(target_dir);
    std::fs::create_dir_all(&target_workspace)?;
    std::fs::create_dir_all(target_meta.join("checkpoint-blobs"))?;

    let mut latest_files = BTreeMap::<Utf8PathBuf, String>::new();
    for event in &selected {
        if event.get("t").and_then(Value::as_str) != Some("step_finished") {
            continue;
        }
        let Some(files) = event.get("files").and_then(Value::as_array) else {
            continue;
        };
        for file in files {
            let path = file
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| ServiceError::Invalid("checkpoint file pin has no path".into()))?;
            let path = Utf8PathBuf::from(path);
            if !is_safe_relative_path(path.as_str()) {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint file path `{path}` is unsafe"
                )));
            }
            let digest = file.get("sha256").and_then(Value::as_str).ok_or_else(|| {
                ServiceError::Invalid(format!("checkpoint file `{path}` has no sha256"))
            })?;
            latest_files.insert(path, digest.to_string());
        }
    }
    for (path, digest) in latest_files {
        let source_blob = source_meta.join("checkpoint-blobs").join(&digest);
        let source = if source_blob.is_file() {
            source_blob
        } else {
            let current = run_workspace_dir(source_dir).join(&path);
            if !current.is_file() {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable; the source run predates checkpoint snapshots"
                )));
            }
            if qcg_policy::hash_file_sha256(&current, None)
                .map(|(hex, _)| hex)
                .map_err(|_| {
                    ServiceError::Invalid(format!(
                        "checkpoint blob `{digest}` for `{path}` is unavailable; the source run predates checkpoint snapshots"
                    ))
                })? != digest
            {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable and the current workspace contains a different revision"
                )));
            }
            current
        };
        if qcg_policy::hash_file_sha256(&source, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification"
            )));
        }
        let destination = target_workspace.join(&path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&source, &destination)?;
        if qcg_policy::hash_file_sha256(&destination, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was copied"
            )));
        }
        std::fs::copy(
            &destination,
            target_meta.join("checkpoint-blobs").join(&digest),
        )?;
    }

    let journal_path = target_meta.join("journal.jsonl");
    let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
    let mut next_seq = 0_u64;
    for mut event in selected {
        next_seq = next_seq.saturating_add(1);
        let object = event.as_object_mut().ok_or_else(|| {
            ServiceError::Invalid("checkpoint journal event is not an object".into())
        })?;
        object.insert("seq".into(), Value::Number(next_seq.into()));
        object.insert("run_id".into(), Value::String(target_id.to_string()));
        object.insert(
            "trace_id".into(),
            Value::String(qcg_api::trace_id_for_run(target_id)),
        );
        object.insert(
            "span_id".into(),
            Value::String(qcg_api::span_id_for_seq(next_seq)),
        );
        if object.contains_key("parent_span_id") {
            let scope = object
                .get("node")
                .and_then(Value::as_str)
                .map(|node| format!("step:{node}"))
                .unwrap_or_else(|| "run".to_string());
            object.insert(
                "parent_span_id".into(),
                Value::String(qcg_api::span_id_for_scope(target_id, &scope)),
            );
        }
        serde_json::to_writer(&mut journal, &event)?;
        journal.write_all(b"\n")?;
    }
    next_seq = next_seq.saturating_add(1);
    let fork_event = json!({
        "t": "run_forked",
        "ts": chrono::Utc::now().to_rfc3339(),
        "seq": next_seq,
        "run_id": target_id,
        "trace_id": qcg_api::trace_id_for_run(target_id),
        "span_id": qcg_api::span_id_for_seq(next_seq),
        "source_run_id": source_id,
        "source_seq": at_seq,
    });
    serde_json::to_writer(&mut journal, &fork_event)?;
    journal.write_all(b"\n")?;
    if !patch.inputs.is_empty() || !patch.step_outputs.is_empty() || !patch.step_statuses.is_empty()
    {
        next_seq = next_seq.saturating_add(1);
        let patch_event = json!({
            "t": "state_patched",
            "ts": chrono::Utc::now().to_rfc3339(),
            "seq": next_seq,
            "run_id": target_id,
            "trace_id": qcg_api::trace_id_for_run(target_id),
            "span_id": qcg_api::span_id_for_seq(next_seq),
            "inputs": patch.inputs,
            "step_outputs": patch.step_outputs,
            "step_statuses": patch.step_statuses,
        });
        serde_json::to_writer(&mut journal, &patch_event)?;
        journal.write_all(b"\n")?;
    }
    journal.sync_all()?;
    let state = RunState::fold_journal(&journal_path)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    state
        .persist_atomic(&target_meta.join("state.json"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    Ok(())
}

pub(crate) fn journal_is_empty(path: &Utf8Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.len() == 0)
}

pub(crate) fn lock_direct_run(metadata_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(metadata_dir)?;
    let lock_path = metadata_dir.join(".run.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    file.try_lock_exclusive().map_err(|error| {
        if is_lock_contention(&error) {
            ServiceError::Invalid(format!(
                "output metadata `{metadata_dir}` is already active in another qcg run"
            ))
        } else {
            ServiceError::Io(error)
        }
    })?;
    Ok(file)
}

pub fn direct_run_meta_dir(workspace: &Utf8Path) -> Utf8PathBuf {
    let run_id = direct_run_id(workspace);
    workspace
        .parent()
        .unwrap_or_else(|| Utf8Path::new("."))
        .join(".qcg/runs")
        .join(&run_id)
        .join("meta")
}

/// Best-effort warning when a direct execution shares its runs directory
/// with a live server process. Cross-process runs coordinate only through
/// provider quotas; the server's `max_active_runs` does not apply here.
pub(crate) fn warn_if_shared_runs_dir_owned(workspace: &Utf8Path) {
    let lock_path = workspace
        .parent()
        .unwrap_or_else(|| Utf8Path::new("."))
        .join(".qcg/runs/.service.lock");
    let lock_file = match OpenOptions::new().read(true).write(true).open(&lock_path) {
        Ok(lock_file) => lock_file,
        Err(_) => return,
    };
    match lock_file.try_lock_exclusive() {
        Ok(_) => {}
        Err(error) if is_lock_contention(&error) => {
            tracing::warn!(
                workspace = %workspace,
                "runs directory is owned by a running qcg server; direct execution bypasses its max_active_runs limit"
            );
        }
        Err(_) => {}
    }
}

pub(crate) fn app_registry(runtime: Arc<qcg_llm::LlmRuntime>) -> qcg_engine::StepRegistry {
    let mut registry = qcg_steps::deterministic_registry_with_mcp(Arc::new(runtime.mcp.clone()));
    qcg_llm_steps::register_llm_steps(&mut registry, runtime);
    registry
}

/// Loads the LLM runtime for step registry assembly, falling back to
/// built-in capabilities when no providers registry is found. This is the
/// single choke point for registry assembly; binaries resolve the registry
/// through here instead of composing runtimes directly.
pub fn load_llm_runtime(
    providers_path: Option<&Utf8Path>,
) -> Result<Arc<qcg_llm::LlmRuntime>, ServiceError> {
    match qcg_llm::LlmRouter::load_optional(providers_path) {
        Ok(Some(router)) => Ok(Arc::new(router.into_runtime())),
        // No registry was found: built-in capabilities stay available
        // while other ids receive setup guidance during validation.
        Ok(None) => Ok(Arc::new(qcg_llm::LlmRuntime::builtins())),
        Err(error) => Err(ServiceError::Invalid(error.to_string())),
    }
}

/// Assembles the full application step registry, resolving the LLM runtime
/// from an explicit providers path or the standard search locations.
pub fn app_registry_with_providers(
    providers_path: Option<&Utf8Path>,
) -> Result<qcg_engine::StepRegistry, ServiceError> {
    Ok(app_registry(load_llm_runtime(providers_path)?))
}

pub fn built_in_step_param_schemas() -> BTreeMap<String, Value> {
    let mut registry = deterministic_registry();
    qcg_llm_steps::register_fake_llm_steps(&mut registry);
    registry.params_schemas()
}

pub fn step_param_schemas_markdown() -> Result<String, ServiceError> {
    let mut markdown = String::new();
    for (step_type, schema) in built_in_step_param_schemas() {
        markdown.push_str("### `");
        markdown.push_str(&step_type);
        markdown.push_str("`\n\n```json\n");
        markdown.push_str(&serde_json::to_string_pretty(&schema)?);
        markdown.push_str("\n```\n\n");
    }
    Ok(markdown)
}
