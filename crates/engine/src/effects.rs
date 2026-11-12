//! Run-journal ledger for container effects: the intent is durable before the
//! create call, so a crash at any later point leaves a journaled unique name
//! that recovery can reclaim (see `docs/container-crash-recovery.md`).

use camino::{Utf8Path, Utf8PathBuf};
use serde_json::json;

use crate::{JournalLimits, JournalWriter};

#[derive(Debug)]
pub struct JournalEffects {
    journal: Utf8PathBuf,
    run_id: String,
    limits: JournalLimits,
    store: String,
}

impl JournalEffects {
    /// `metadata_dir` is `<runs>/<run>/meta`; the store is identified by the
    /// canonical runs directory so one store's labels never read as another's.
    pub fn new(metadata_dir: &Utf8Path, run_id: &str, limits: JournalLimits) -> Self {
        let runs = metadata_dir.parent().and_then(Utf8Path::parent);
        let store = runs
            .map(|dir| dir.canonicalize_utf8().unwrap_or_else(|_| dir.to_owned()))
            .map(|dir| {
                use sha2::{Digest, Sha256};
                hex::encode(&Sha256::digest(dir.as_str().as_bytes())[..16])
            })
            .unwrap_or_default();
        Self {
            journal: metadata_dir.join("journal.jsonl"),
            run_id: run_id.to_string(),
            limits,
            store,
        }
    }

    fn append(&self, kind: &str, payload: serde_json::Value) -> std::io::Result<()> {
        JournalWriter::append_single_event(
            &self.journal,
            &self.run_id,
            kind,
            payload,
            self.limits,
            None,
        )
        .map(drop)
        .map_err(std::io::Error::other)
    }
}

impl container::EffectLedger for JournalEffects {
    fn labels(&self, name: &str) -> Vec<(String, String)> {
        [
            ("qcg.store", &self.store),
            ("qcg.run", &self.run_id),
            ("qcg.effect", &name.to_string()),
        ]
        .map(|(key, value)| (key.to_string(), value.clone()))
        .to_vec()
    }

    fn open(&self, backend: &container::Backend, name: &str) -> std::io::Result<()> {
        self.append("effect_intent", json!({"effect": name, "backend": backend}))
    }

    fn close(&self, name: &str, outcome: &str) -> std::io::Result<()> {
        self.append("effect_closed", json!({"effect": name, "outcome": outcome}))
    }
}
