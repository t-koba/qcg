use camino::{Utf8Path, Utf8PathBuf};
use qcg_types::{FileValue, OutputArtifact};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub struct RunSummary {
    pub run_id: String,
    pub status: String,
    pub generator: String,
    pub generator_path: String,
    pub contract_sha256: String,
    pub inputs: BTreeMap<String, Value>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub artifacts: Vec<OutputArtifact>,
    pub retain_days: Option<u32>,
}

impl RunSummary {
    pub fn to_json(&self) -> Value {
        json!({
            "run_id": self.run_id,
            "status": self.status,
            "generator": self.generator,
            "generator_path": self.generator_path,
            "contract_sha256": self.contract_sha256,
            "inputs": summarize_file_values(&self.inputs),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "artifacts": self.artifacts,
            "retain_days": self.retain_days,
        })
    }
}

fn summarize_file_values(inputs: &BTreeMap<String, Value>) -> Value {
    Value::Object(
        inputs
            .iter()
            .map(|(field, value)| {
                let summary = FileValue::from_value(value)
                    .and_then(|file| {
                        let bytes = file.decode()?;
                        Ok(json!({
                            "name": file.name,
                            "bytes": bytes.len(),
                            "sha256": hex::encode(Sha256::digest(&bytes)),
                        }))
                    })
                    .unwrap_or_else(|_| value.clone());
                (field.clone(), summary)
            })
            .collect(),
    )
}

pub fn run_meta_dir(run_dir: &Utf8Path) -> Utf8PathBuf {
    run_dir.join("meta")
}

pub fn run_workspace_dir(run_dir: &Utf8Path) -> Utf8PathBuf {
    run_dir.join("workspace")
}
