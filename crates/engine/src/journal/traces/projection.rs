//! Optional fork evidence, not continuation authority. The service owns the
//! ordinary fork and calls publication only after both child streams are durable.
use super::*;

struct Entry {
    record: Record,
    source_payload: Option<[u8; 32]>,
    bound: bool,
}

// Only these identity fields are rewritten by the fork. Parent-span presence
// is preserved: a missing parent is not interchangeable with an added one.
fn payload_digest(event: &Value) -> Option<[u8; 32]> {
    let mut payload = event.clone();
    let object = payload.as_object_mut()?;
    for key in ["seq", "run_id", "trace_id", "span_id"] {
        object.remove(key);
    }
    if let Some(parent) = object.get_mut("parent_span_id") {
        *parent = Value::Null;
    }
    digest(&payload)
}

/// A bounded, private trace projection for an exclusively owned fresh fork.
/// Missing or invalid evidence disables the entire optional projection. This
/// does not authorize continuation, reset, or reuse of any recorded outcome.
pub struct ForkTraceProjection {
    records: BTreeMap<u64, Entry>,
    limits: JournalLimits,
    last_target_seq: u64,
}

impl ForkTraceProjection {
    /// `selected` must contain only selected durable source journal events,
    /// never audit records. Source sequence ties must not choose a winner.
    pub fn new(
        source_journal: &Utf8Path,
        selected: &[Value],
        cutoff: u64,
        limits: JournalLimits,
    ) -> Option<Self> {
        let limits = bounded(limits);
        let (records, _) = TraceJournal::scan(source_journal, limits)?;
        let mut records: BTreeMap<_, _> = records
            .into_iter()
            .filter(|record| record.seq <= cutoff)
            .map(|record| {
                (
                    record.seq,
                    Entry {
                        record,
                        source_payload: None,
                        bound: false,
                    },
                )
            })
            .collect();
        if records.is_empty() {
            return None;
        }
        for event in selected {
            let Some(entry) = event["seq"].as_u64().and_then(|seq| records.get_mut(&seq)) else {
                continue;
            };
            if entry.source_payload.is_some() || api::RunEvent::from_flat(event).is_err() {
                return None;
            }
            entry.record.validated_trace(event, limits)?;
            entry.source_payload = Some(payload_digest(event)?);
        }
        if records.values().any(|entry| entry.source_payload.is_none()) {
            return None;
        }
        Some(Self {
            records,
            limits,
            last_target_seq: 0,
        })
    }

    /// Bind one selected journal event to the exact event written to the child,
    /// after seq/run/trace/span rewriting. On failure the caller must discard
    /// the projection; records are never rebound to audit events.
    pub fn bind(&mut self, source_seq: u64, child: &Value) -> Option<()> {
        let Some(entry) = self.records.get_mut(&source_seq) else {
            return Some(());
        };
        let seq = child["seq"].as_u64()?;
        if entry.bound
            || seq <= self.last_target_seq
            || api::RunEvent::from_flat(child).is_err()
            || payload_digest(child) != entry.source_payload
        {
            return None;
        }
        let record = &mut entry.record;
        record.seq = seq;
        record.event_digest = digest(child)?;
        record.record_digest = record.calculate_digest()?;
        record.validated_trace(child, self.limits)?;
        entry.bound = true;
        self.last_target_seq = seq;
        Some(())
    }

    /// Best effort and all-or-nothing. Caller has already synced the child
    /// journal, audit and their directory, and still exclusively owns the fork.
    /// No incomplete temporary output is retained after an ordinary I/O failure.
    pub fn publish(self, child_journal: &Utf8Path) {
        if self.records.values().any(|entry| !entry.bound) {
            return;
        }
        let target = child_journal.with_file_name(FILE_NAME);
        // A fork is fresh. Never replace pre-existing private evidence.
        if !matches!(std::fs::symlink_metadata(&target), Err(e) if e.kind() == std::io::ErrorKind::NotFound)
        {
            return;
        }
        let temp = target.with_file_name(format!("{FILE_NAME}.part-{}", uuid::Uuid::now_v7()));
        let Ok(mut file) = open_private_trace(&temp, TraceAccess::CreateNew) else {
            return;
        };
        let written = (|| -> Result<(), super::super::JournalError> {
            let mut stats = JournalStats::default();
            for entry in self.records.values() {
                let record = &entry.record;
                let bytes =
                    serialize_bounded(record, self.limits.max_event_bytes, "dependency trace")?;
                super::super::append_serialized_json_line(
                    &mut file,
                    bytes,
                    &mut stats,
                    self.limits,
                )?;
            }
            file.sync_all()?;
            Ok(())
        })();
        drop(file);
        if written.is_ok()
            && std::fs::rename(&temp, &target).is_ok()
            && super::super::writer::sync_parent_dir(&target).is_err()
        {
            let _ = std::fs::remove_file(&target);
        }
        let _ = std::fs::remove_file(&temp);
    }
}
