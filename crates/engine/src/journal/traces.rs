//! Private, optional continuation evidence. Never part of RunEvent or state.json.
//! The durable journal lock owns both streams. A crash between outcome and trace
//! leaves missing evidence, not a different ordinary replay result.
use super::{JournalLimits, JournalStats, serialize_bounded};
use camino::Utf8Path;
use contract::{ReadIdentity, ReadKind, ReadOwner, ReadRecorder, ReadTrace};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
mod private_file;
use private_file::{Access as TraceAccess, open as open_private_trace};
use std::io::{Read, Seek, SeekFrom, Write};

// Wire-format version, not a configurable policy limit.
const VERSION: u32 = 1;
const FILE_NAME: &str = ".dependency-traces.jsonl";

#[derive(Debug, Clone)]
pub(crate) struct OutcomeTrace {
    pub(crate) seq: u64,
    pub(crate) event_digest: [u8; 32],
    pub(crate) trace: ReadTrace,
}

/// Kept only in memory, bounded independently of the ordinary state cache.
/// Absence always means incomplete; deserialization can never grant evidence.
#[derive(Debug, Clone, Default)]
pub(crate) struct OutcomeTraces {
    pub(crate) owners: BTreeMap<String, OutcomeTrace>,
    bytes: usize,
}

impl OutcomeTraces {
    pub(crate) fn remove(&mut self, owner: &str) {
        if let Some(old) = self.owners.remove(owner) {
            self.bytes -= Self::charge(owner, &old);
        }
    }

    fn charge(owner: &str, trace: &OutcomeTrace) -> usize {
        owner.len() + std::mem::size_of::<(String, OutcomeTrace)>() + trace.trace.retained_bytes
    }

    fn insert(&mut self, owner: String, trace: OutcomeTrace, limit: usize) {
        self.remove(&owner);
        let charge = Self::charge(&owner, &trace);
        if charge <= limit.saturating_sub(self.bytes) {
            self.bytes += charge;
            self.owners.insert(owner, trace);
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    seq: u64,
    event_digest: [u8; 32],
    owner: String,
    complete: bool,
    reads: Vec<Dependency>,
    record_digest: [u8; 32],
}

#[derive(Serialize)]
struct RecordContents<'a> {
    version: u32,
    seq: u64,
    event_digest: [u8; 32],
    owner: &'a str,
    complete: bool,
    reads: &'a [Dependency],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dependency {
    path: Vec<String>,
    kind: Kind,
    identity: Identity,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Lookup,
    Collection,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Identity {
    Present,
    Digest([u8; 32]),
    Missing,
    Unsupported,
}

/// Owner whose existing evidence this event supersedes, including starts and
/// interruptions. Iteration identity is NOT the last child in `owner`.
pub(crate) fn trace_owner(event: &Value) -> Option<String> {
    match event["t"].as_str()? {
        "step_started" | "step_finished" | "step_skipped" | "step_interrupted"
        | "confirm_request" => event["node"].as_str().map(str::to_owned),
        "foreach_iteration_settled" | "foreach_sibling_drained" => {
            event["iteration"].as_str().map(str::to_owned)
        }
        "foreach_iteration" => Some(format!(
            "{}[{}]",
            event["node"].as_str()?,
            event["index"].as_u64()?
        )),
        _ => None,
    }
}

fn terminal(event: &Value) -> bool {
    match event["t"].as_str() {
        Some("step_finished") => matches!(
            event["status"].as_str(),
            Some(
                "success"
                    | "repaired"
                    | "routed"
                    | "answered_on_fail"
                    | "regenerated"
                    | "repair_exhausted"
                    | "regenerate_exhausted"
                    | "check_failed"
                    | "failed"
            )
        ),
        Some("step_skipped") => true,
        Some("foreach_iteration_settled" | "foreach_sibling_drained") => {
            let (Some(node), Some(index), Some(iteration)) = (
                event["node"].as_str(),
                event["index"].as_u64(),
                event["iteration"].as_str(),
            ) else {
                return false;
            };
            iteration == format!("{node}[{index}]")
                && matches!(
                    event["classification"].as_str(),
                    Some("completed_success" | "completed_terminal" | "task_error")
                )
                && !matches!(
                    event["outcome"].as_str(),
                    Some("needs_user" | "needs_confirm")
                )
        }
        _ => false,
    }
}

fn digest(event: &Value) -> Option<[u8; 32]> {
    match ReadIdentity::of(Some(event)) {
        ReadIdentity::Digest(digest) => Some(digest),
        _ => None,
    }
}

// Reuse the configured journal/state policies, with their documented defaults
// even for callers using unbounded ordinary journal reads.
fn bounded(mut limits: JournalLimits) -> JournalLimits {
    let defaults = JournalLimits::default();
    limits.max_event_bytes = limits.max_event_bytes.or(defaults.max_event_bytes);
    limits.max_total_bytes = limits.max_total_bytes.or(defaults.max_total_bytes);
    limits.max_event_count = limits.max_event_count.or(defaults.max_event_count);
    limits.max_state_bytes = limits.max_state_bytes.or(defaults.max_state_bytes);
    limits
}

impl Record {
    fn capture(event: &Value, reads: Option<&ReadRecorder>) -> Option<Self> {
        let owner = trace_owner(event)?;
        if !terminal(event) {
            return None;
        }
        let scope = ReadOwner::Node(owner.clone());
        let reads = reads?.complete_reads_for(&scope);
        let complete = reads.is_some();
        let reads = reads
            .into_iter()
            .flatten()
            .map(|read| Dependency {
                path: read.path,
                kind: match read.kind {
                    ReadKind::Lookup => Kind::Lookup,
                    ReadKind::Collection => Kind::Collection,
                },
                identity: match read.identity {
                    ReadIdentity::Present => Identity::Present,
                    ReadIdentity::Digest(value) => Identity::Digest(value),
                    ReadIdentity::Missing => Identity::Missing,
                    ReadIdentity::Unsupported => Identity::Unsupported,
                },
            })
            .collect();
        let mut record = Self {
            version: VERSION,
            seq: event["seq"].as_u64()?,
            event_digest: digest(event)?,
            owner,
            complete,
            reads,
            record_digest: [0; 32],
        };
        record.record_digest = record.calculate_digest()?;
        Some(record)
    }

    fn calculate_digest(&self) -> Option<[u8; 32]> {
        struct HashWriter(Sha256);
        impl Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let contents = RecordContents {
            version: self.version,
            seq: self.seq,
            event_digest: self.event_digest,
            owner: &self.owner,
            complete: self.complete,
            reads: &self.reads,
        };
        let mut writer = HashWriter(Sha256::new());
        writer.0.update(b"qcg-private-dependency-trace-v1\0");
        serde_json::to_writer(&mut writer, &contents).ok()?;
        Some(writer.0.finalize().into())
    }

    fn install(self, traces: &mut OutcomeTraces, event: &Value, limits: JournalLimits) {
        if self.calculate_digest() != Some(self.record_digest)
            || self.version != VERSION
            || !self.complete
            || !terminal(event)
            || trace_owner(event).as_deref() != Some(self.owner.as_str())
        {
            return;
        }
        // Re-charge parsed dependencies rather than trusting serialized counts
        // or flags. Unsupported identities and memory overflow stay incomplete.
        let owner = ReadOwner::Node(self.owner.clone());
        let recorder =
            ReadRecorder::new(limits.max_event_bytes.unwrap_or_default()).for_owner(owner.clone());
        for read in self.reads {
            recorder.record(
                &read.path,
                match read.kind {
                    Kind::Lookup => ReadKind::Lookup,
                    Kind::Collection => ReadKind::Collection,
                },
                match read.identity {
                    Identity::Present => ReadIdentity::Present,
                    Identity::Digest(value) => ReadIdentity::Digest(value),
                    Identity::Missing => ReadIdentity::Missing,
                    Identity::Unsupported => ReadIdentity::Unsupported,
                },
            );
        }
        let trace = recorder.snapshot();
        let trace = OutcomeTrace {
            seq: self.seq,
            event_digest: self.event_digest,
            trace,
        };
        if trace.trace.complete_for(&owner)
            && event["seq"].as_u64() == Some(trace.seq)
            && digest(event) == Some(trace.event_digest)
        {
            traces.insert(
                self.owner,
                trace,
                limits.max_state_bytes.unwrap_or_default(),
            );
        }
    }
}

/// A disabled stream stays disabled after an I/O/limit error. No new public
/// event is emitted and no ordinary outcome is changed by evidence failures.
pub(crate) struct TraceJournal {
    stats: Option<JournalStats>,
}

impl TraceJournal {
    fn scan(path: &Utf8Path, limits: JournalLimits) -> Option<(Vec<Record>, JournalStats)> {
        let path = path.with_file_name(FILE_NAME);
        // Unlike the public crash-repair reader, evidence must be newline
        // terminated. Never ignore a partial final record or append after it.
        let mut file = match open_private_trace(&path, TraceAccess::Read) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Some((Vec::new(), JournalStats::default()));
            }
            Err(_) => return None,
        };
        if file.metadata().ok()?.len() > 0 {
            file.seek(SeekFrom::End(-1)).ok()?;
            let mut last = [0];
            file.read_exact(&mut last).ok()?;
            if last != [b'\n'] {
                return None;
            }
        }
        file.seek(SeekFrom::Start(0)).ok()?;
        let scan = super::read::read_journal_file(file, None, limits).ok()?;
        let records = scan
            .events
            .into_iter()
            .map(serde_json::from_value::<Record>)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let mut seen = BTreeSet::new();
        if records.iter().any(|record| !seen.insert(record.seq)) {
            return None;
        }
        Some((records, scan.stats))
    }

    pub(crate) fn restore(
        state: &mut crate::RunState,
        events: &[Value],
        path: &Utf8Path,
        limits: JournalLimits,
    ) -> Self {
        state.dependency_traces = OutcomeTraces::default();
        let limits = bounded(limits);
        let Some((records, stats)) = Self::scan(path, limits) else {
            return Self { stats: None };
        };
        let mut latest = BTreeMap::new();
        for event in events {
            if let Some(owner) = trace_owner(event) {
                latest.remove(&owner);
                if terminal(event) && api::RunEvent::from_flat(event).is_ok() {
                    latest.insert(owner, event);
                }
            }
        }
        for record in records {
            if let Some(event) = latest.get(&record.owner) {
                record.install(&mut state.dependency_traces, event, limits);
            }
        }
        Self { stats: Some(stats) }
    }

    /// Caller holds the durable journal lock, including across processes.
    pub(crate) fn persist(
        &mut self,
        state: &mut crate::RunState,
        event: &Value,
        recorder: Option<&ReadRecorder>,
        path: &Utf8Path,
        limits: JournalLimits,
        durable_file: &std::fs::File,
    ) {
        if self.stats.is_none() {
            return;
        }
        let Some(record) = Record::capture(event, recorder) else {
            return;
        };
        let limits = bounded(limits);
        let result = (|| -> Result<(), super::JournalError> {
            let stats = self
                .stats
                .as_mut()
                .ok_or_else(|| std::io::Error::other("trace stream disabled"))?;
            let trace_path = path.with_file_name(FILE_NAME);
            let mut file = open_private_trace(&trace_path, TraceAccess::Append)?;
            let len = file.metadata()?.len();
            if len < stats.bytes as u64 {
                return Err(std::io::Error::other("trace stream shrank").into());
            }
            if usize::try_from(len).ok() != Some(stats.bytes) {
                // A peer appended under the same journal lock. Recount only
                // when changed, not on every outcome; never repair evidence.
                *stats = Self::scan(path, limits)
                    .ok_or_else(|| std::io::Error::other("invalid trace stream"))?
                    .1;
            }
            let bytes = serialize_bounded(&record, limits.max_event_bytes, "dependency trace")?;
            // A private record must never become durable ahead of its outcome.
            // Failure of this extra sync only removes continuation evidence.
            durable_file.sync_data()?;
            super::append_serialized_json_line(&mut file, bytes, stats, limits)?;
            file.sync_data()?;
            super::writer::sync_parent_dir(&trace_path)?;
            Ok(())
        })();
        if result.is_ok() {
            record.install(&mut state.dependency_traces, event, limits);
        } else {
            self.stats = None;
            state.dependency_traces = OutcomeTraces::default();
        }
    }
}

#[cfg(test)]
mod tests;
