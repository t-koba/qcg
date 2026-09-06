use crate::{FilePin, JournalWriter, NodeOutcome, RunState, StepError};
use qcg_contract::RuntimeLimits;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::checkpoint::{CheckpointAccounting, hash_file};
use super::types::{EngineError, RunContext};

#[derive(Debug)]
pub(crate) struct JournalReplay {
    pub(crate) steps: BTreeMap<String, ReplayedStep>,
    pub(crate) state: RunState,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayedStep {
    pub(crate) status: String,
    pub(crate) output: Option<Value>,
    pub(crate) files: Vec<FilePin>,
}

impl JournalReplay {
    pub(crate) fn from_state(state: RunState) -> Self {
        let steps = state
            .nodes
            .iter()
            .filter_map(|(path, outcome)| match outcome {
                NodeOutcome::Success { output, files } => Some((
                    path.as_str().to_string(),
                    ReplayedStep {
                        status: "success".into(),
                        output: output.clone(),
                        files: files.clone(),
                    },
                )),
                NodeOutcome::Skipped { .. } | NodeOutcome::Failed { .. } => None,
            })
            .collect();
        Self { steps, state }
    }

    pub(crate) fn verify_files(
        &self,
        workspace: &camino::Utf8Path,
        limits: &RuntimeLimits,
        accounting: &Arc<Mutex<CheckpointAccounting>>,
    ) -> Result<(), EngineError> {
        for (path, step) in &self.steps {
            step.verify_files(workspace, limits, accounting)
                .map_err(|message| {
                    EngineError::Failed(format!("cannot safely resume node `{path}`: {message}"))
                })?;
        }
        Ok(())
    }
}

impl ReplayedStep {
    fn verify_files(
        &self,
        workspace: &camino::Utf8Path,
        limits: &RuntimeLimits,
        accounting: &Arc<Mutex<CheckpointAccounting>>,
    ) -> Result<(), String> {
        for pin in &self.files {
            if pin.path.is_absolute() {
                return Err(format!(
                    "journal contains absolute output path `{}`",
                    pin.path
                ));
            }
            let candidate = workspace.join(&pin.path);
            let digest = hash_file(&candidate, limits.output_file_limit_bytes)
                .map_err(|error| format!("output `{}` is unavailable: {error}", pin.path))?;
            if digest.sha256 != pin.sha256 {
                return Err(format!(
                    "output `{}` digest changed: expected {}, got {}",
                    pin.path, pin.sha256, digest.sha256
                ));
            }
            let mut accounting = accounting
                .lock()
                .map_err(|_| "checkpoint accounting mutex was poisoned".to_owned())?;
            accounting
                .record(&pin.path, digest.bytes, limits)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ExecutionEnv<'a> {
    pub(crate) context: &'a RunContext,
    pub(crate) journal: &'a JournalWriter,
}

#[derive(Clone)]
pub(crate) struct BudgetTracker {
    pub(crate) max_total_steps: usize,
    pub(crate) executed_steps: Arc<AtomicUsize>,
}

impl BudgetTracker {
    pub(crate) fn new(max_total_steps: usize, executed_steps: usize) -> Self {
        Self {
            max_total_steps: max_total_steps.max(1),
            executed_steps: Arc::new(AtomicUsize::new(executed_steps)),
        }
    }

    pub(crate) fn consume(&self, node_id: &str) -> Result<(), StepError> {
        let result =
            self.executed_steps
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |executed| {
                    (executed < self.max_total_steps).then_some(executed.saturating_add(1))
                });
        if let Err(executed) = result {
            return Err(StepError::failed(
                node_id,
                format!(
                    "global step budget exceeded: {} >= {}",
                    executed, self.max_total_steps
                ),
            ));
        }
        Ok(())
    }
}
