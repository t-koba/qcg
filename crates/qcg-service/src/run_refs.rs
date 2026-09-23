//! Run-reference resolution: turn a `run_ref` resource selector into an
//! immutable, hash-pinned material for one execution.
//!
//! Resolution is policy (which run, which declared artifact) and happens
//! once per run; the result is persisted under the run metadata so a resume
//! never depends on the source run surviving retention. The engine only
//! consumes the material and copies it into its own workspace.

use std::collections::BTreeMap;

use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::ApiError;
use qcg_contract::{Contract, ResourceKind};
use qcg_engine::RunRefMaterial;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

/// Persisted resolution result: the bytes live beside this index, so resume
/// reads them without touching the source run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedRunRef {
    source_run_id: String,
    artifact: String,
    sha256: String,
    bytes: usize,
}

const RUN_REF_INDEX: &str = "run-refs.json";
const RUN_REF_DIR: &str = "run-refs";

/// Loads persisted run references for this run, or resolves and persists
/// them on first execution. An unresolved reference fails explicitly; there
/// is no fallback to a different run or artifact.
pub(crate) fn load_or_resolve_run_refs(
    runs_dir: &Utf8Path,
    contract: &Contract,
    metadata_dir: &Utf8Path,
) -> Result<BTreeMap<String, RunRefMaterial>, ApiError> {
    let referenced: Vec<(&str, &qcg_contract::ResourceDef)> = contract
        .manifest
        .resources
        .iter()
        .filter(|(_, resource)| resource.kind == ResourceKind::RunRef)
        .map(|(name, resource)| (name.as_str(), resource))
        .collect();
    if referenced.is_empty() {
        return Ok(BTreeMap::new());
    }
    if let Some(persisted) = load_persisted(metadata_dir, &referenced)? {
        return Ok(persisted);
    }
    let mut resolved = BTreeMap::new();
    let mut index = BTreeMap::new();
    for (name, resource) in &referenced {
        let params = resource
            .run_ref_params(name)
            .map_err(|error| ApiError::internal(error.to_string()))?;
        let source = resolve_source_run(runs_dir, &params.selector.single())
            .map_err(|reason| ApiError::invalid_field(*name, reason))?;
        let artifact = read_source_artifact(&source, &params)
            .map_err(|reason| ApiError::invalid_field(*name, reason))?;
        index.insert(
            (*name).to_string(),
            PersistedRunRef {
                source_run_id: source.run_id.clone(),
                artifact: artifact.path.clone(),
                sha256: artifact.sha256.clone(),
                bytes: artifact.bytes.len(),
            },
        );
        resolved.insert(
            (*name).to_string(),
            RunRefMaterial {
                source_run_id: source.run_id,
                artifact: artifact.path,
                sha256: artifact.sha256,
                bytes: artifact.bytes,
            },
        );
    }
    persist(metadata_dir, &resolved).map_err(|error| {
        ApiError::internal(format!("failed to persist run references: {error}"))
    })?;
    Ok(resolved)
}

#[derive(Debug)]
struct SourceRun {
    run_id: String,
    meta_dir: Utf8PathBuf,
}

struct SourceArtifact {
    path: String,
    sha256: String,
    bytes: Vec<u8>,
}

fn resolve_source_run(
    runs_dir: &Utf8Path,
    selector: &Option<(&'static str, &str)>,
) -> Result<SourceRun, String> {
    let Some((kind, value)) = selector else {
        return Err(
            "requires exactly one selector: run_id, latest_success, or latest_terminal".to_string(),
        );
    };
    match *kind {
        "run_id" => {
            let meta_dir = crate::summaries::run_meta_dir(&runs_dir.join(value));
            let summary = crate::summaries::run_summary(&runs_dir.join(value))
                .map_err(|error| format!("source run `{value}` is unreadable: {error}"))?;
            if summary.status != "success" {
                return Err(format!(
                    "source run `{value}` is `{}`; declared artifacts require a successful run",
                    summary.status
                ));
            }
            Ok(SourceRun {
                run_id: value.to_string(),
                meta_dir,
            })
        }
        "latest_success" | "latest_terminal" => {
            let terminal_match = |status: &str| status == "success";
            let mut best: Option<(String, String, Utf8PathBuf)> = None;
            let mut scanned = 0_usize;
            for entry in std::fs::read_dir(runs_dir)
                .map_err(|error| format!("runs directory is unreadable: {error}"))?
            {
                scanned += 1;
                if scanned > qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES {
                    return Err("runs directory scan limit exceeded".to_string());
                }
                let entry =
                    entry.map_err(|error| format!("runs directory is unreadable: {error}"))?;
                let path = Utf8PathBuf::from_path_buf(entry.path())
                    .map_err(|path| format!("run path is not UTF-8: {}", path.display()))?;
                if !path.join("meta").join("journal.jsonl").exists() {
                    continue;
                }
                let summary = match crate::summaries::run_summary(&path) {
                    Ok(summary) => summary,
                    Err(_) => continue,
                };
                // Summaries carry `id@version`; the selector names the id.
                let generator_id = summary
                    .generator
                    .split_once('@')
                    .map(|(id, _)| id)
                    .unwrap_or(summary.generator.as_str());
                if generator_id != *value {
                    continue;
                }
                let matched = if *kind == "latest_success" {
                    summary.status == "success"
                } else {
                    terminal_match(&summary.status)
                        || matches!(
                            summary.status.as_str(),
                            "failed" | "canceled" | "interrupted"
                        )
                };
                if !matched {
                    continue;
                }
                let candidate = (
                    summary.started_at.clone(),
                    summary.run_id.clone(),
                    path.join("meta"),
                );
                if best
                    .as_ref()
                    .is_none_or(|(started, _, _)| candidate.0 > *started)
                {
                    best = Some(candidate);
                }
            }
            let Some((_, run_id, meta_dir)) = best else {
                return Err(format!(
                    "no {} run of generator `{value}` is available",
                    if *kind == "latest_success" {
                        "successful"
                    } else {
                        "terminal"
                    }
                ));
            };
            Ok(SourceRun { run_id, meta_dir })
        }
        other => Err(format!("unsupported selector `{other}`")),
    }
}

fn read_source_artifact(
    source: &SourceRun,
    params: &qcg_contract::RunRefParams,
) -> Result<SourceArtifact, String> {
    let manifest = qcg_engine::read_output_manifest(&source.meta_dir).map_err(|error| {
        format!(
            "source run `{}` has no output manifest: {error}",
            source.run_id
        )
    })?;
    let declared = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.path == params.artifact)
        .ok_or_else(|| {
            format!(
                "source run `{}` does not declare artifact `{}`",
                source.run_id, params.artifact
            )
        })?;
    let workspace = source
        .meta_dir
        .parent()
        .ok_or_else(|| "source run metadata has no parent".to_string())?
        .join("workspace");
    let resolved = qcg_engine::resolve_artifact_path(&workspace, &declared.path)
        .map_err(|error| format!("source artifact path is unsafe: {error}"))?;
    let bytes = qcg_fs::read_bounded(&resolved, Some(params.max_bytes)).map_err(|error| {
        format!(
            "source artifact `{}` could not be read within {} bytes: {error}",
            params.artifact, params.max_bytes
        )
    })?;
    let sha256 = hex::encode(sha2::Sha256::digest(&bytes));
    if sha256 != declared.sha256 {
        return Err(format!(
            "source artifact `{}` does not match its manifest digest",
            params.artifact
        ));
    }
    if let Some(required) = &params.require_sha256
        && sha256 != *required
    {
        return Err(format!(
            "source artifact `{}` does not match the declared require_sha256",
            params.artifact
        ));
    }
    Ok(SourceArtifact {
        path: declared.path.clone(),
        sha256,
        bytes,
    })
}

fn load_persisted(
    metadata_dir: &Utf8Path,
    referenced: &[(&str, &qcg_contract::ResourceDef)],
) -> Result<Option<BTreeMap<String, RunRefMaterial>>, ApiError> {
    let index_path = metadata_dir.join(RUN_REF_INDEX);
    let bytes = match std::fs::read(&index_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ApiError::internal(format!(
                "run reference index is unreadable: {error}"
            )));
        }
    };
    let index: BTreeMap<String, PersistedRunRef> = serde_json::from_slice(&bytes)
        .map_err(|error| ApiError::internal(format!("run reference index is corrupt: {error}")))?;
    let mut materials = BTreeMap::new();
    for (name, _resource) in referenced {
        let Some(entry) = index.get(*name) else {
            return Err(ApiError::internal(format!(
                "run reference `{name}` is missing from the persisted index"
            )));
        };
        let path = metadata_dir.join(RUN_REF_DIR).join(format!("{name}.bin"));
        let bytes = std::fs::read(&path).map_err(|error| {
            ApiError::internal(format!(
                "run reference `{name}` bytes are unreadable: {error}"
            ))
        })?;
        let sha256 = hex::encode(sha2::Sha256::digest(&bytes));
        if sha256 != entry.sha256 || bytes.len() != entry.bytes {
            return Err(ApiError::internal(format!(
                "run reference `{name}` bytes do not match the persisted revision"
            )));
        }
        materials.insert(
            (*name).to_string(),
            RunRefMaterial {
                source_run_id: entry.source_run_id.clone(),
                artifact: entry.artifact.clone(),
                sha256: entry.sha256.clone(),
                bytes,
            },
        );
    }
    Ok(Some(materials))
}

fn persist(
    metadata_dir: &Utf8Path,
    materials: &BTreeMap<String, RunRefMaterial>,
) -> Result<(), std::io::Error> {
    let dir = metadata_dir.join(RUN_REF_DIR);
    std::fs::create_dir_all(&dir)?;
    let mut index = BTreeMap::new();
    for (name, material) in materials {
        std::fs::write(dir.join(format!("{name}.bin")), &material.bytes)?;
        index.insert(
            name.clone(),
            PersistedRunRef {
                source_run_id: material.source_run_id.clone(),
                artifact: material.artifact.clone(),
                sha256: material.sha256.clone(),
                bytes: material.bytes.len(),
            },
        );
    }
    let encoded = serde_json::to_vec_pretty(&index)?;
    let target = metadata_dir.join(RUN_REF_INDEX);
    let staging = target.with_extension("json.tmp");
    std::fs::write(&staging, encoded)?;
    std::fs::rename(&staging, &target)
}
