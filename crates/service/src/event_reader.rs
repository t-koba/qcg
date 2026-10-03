//! Incremental, bounded merge of the durable and observation journals.
use crate::{LocalService, ServiceError, run_meta_dir};
use api::{ApiError, RunEvent};
use camino::Utf8Path;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Seek},
};

pub const EVENT_BATCH_COUNT: usize = 256;
pub const EVENT_BATCH_BYTES: usize = 4 * 1024 * 1024;
#[derive(Debug, Clone, Default)]
pub struct EventCursor {
    offsets: [u64; 2],
    counts: [usize; 2],
    last_seq: u64,
    stream_seq: [u64; 2],
    identities: [Option<(u64, u64)>; 2],
}
pub struct EventBatch {
    pub events: Vec<Value>,
    pub cursor: EventCursor,
    pub exhausted: bool,
}

impl EventCursor {
    pub fn read(&self, directory: &Utf8Path) -> Result<EventBatch, ServiceError> {
        self.read_with_limits(directory, engine::JournalLimits::default())
    }
    pub(crate) fn read_with_limits(
        &self,
        directory: &Utf8Path,
        limits: engine::JournalLimits,
    ) -> Result<EventBatch, ServiceError> {
        let mut cursor = self.clone();
        let paths = [
            run_meta_dir(directory).join("journal.jsonl"),
            run_meta_dir(directory).join("audit.jsonl"),
        ];
        let mut readers = Vec::new();
        for (index, path) in paths.iter().enumerate() {
            let file = match File::open(path) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && index == 1 => None,
                Err(error) => return Err(error.into()),
            };
            readers.push(if let Some(mut file) = file {
                let metadata = file.metadata()?;
                let size = metadata.len();
                #[cfg(unix)]
                let identity = {
                    use std::os::unix::fs::MetadataExt;
                    (metadata.dev(), metadata.ino())
                };
                #[cfg(not(unix))]
                let identity = (0, 0);
                if cursor.identities[index].is_some_and(|prior| prior != identity) {
                    cursor.offsets[index] = 0;
                    cursor.counts[index] = 0;
                    cursor.stream_seq[index] = 0;
                }
                cursor.identities[index] = Some(identity);
                let limit = if index == 0 {
                    limits
                        .max_total_bytes
                        .unwrap_or(policy::DEFAULT_MAX_JOURNAL_TOTAL_BYTES)
                } else {
                    policy::DEFAULT_MAX_AUDIT_TOTAL_BYTES
                };
                if size > limit as u64 {
                    return Err(ServiceError::Invalid(format!(
                        "{} exceeds byte limit",
                        path
                    )));
                }
                if size < cursor.offsets[index] {
                    cursor.offsets[index] = 0;
                    cursor.counts[index] = 0;
                    cursor.stream_seq[index] = 0;
                }
                file.seek(std::io::SeekFrom::Start(cursor.offsets[index]))?;
                Some(BufReader::new(file.take(size - cursor.offsets[index])))
            } else {
                None
            });
        }
        fn next(
            reader: &mut Option<BufReader<std::io::Take<File>>>,
            max_event: usize,
        ) -> Result<Option<(Value, usize)>, ServiceError> {
            let Some(reader) = reader else {
                return Ok(None);
            };
            let mut line = Vec::new();
            let bytes = reader
                .take(max_event.saturating_add(2) as u64)
                .read_until(b'\n', &mut line)?;
            if bytes > max_event.saturating_add(1) {
                return Err(ServiceError::Invalid(
                    "journal event exceeds byte limit".into(),
                ));
            }
            if bytes == 0 || line.last() != Some(&b'\n') {
                return Ok(None);
            }
            let value: Value = serde_json::from_slice(&line)?;
            RunEvent::from_flat(&value).map_err(ServiceError::Invalid)?;
            Ok(Some((value, bytes)))
        }
        let max_event = [
            limits
                .max_event_bytes
                .unwrap_or(policy::DEFAULT_MAX_JOURNAL_EVENT_BYTES),
            policy::DEFAULT_MAX_JOURNAL_EVENT_BYTES,
        ];
        let mut heads = [
            next(&mut readers[0], max_event[0])?,
            next(&mut readers[1], max_event[1])?,
        ];
        let mut events = Vec::new();
        let mut bytes = 0;
        while heads.iter().any(Option::is_some) {
            let index = match (&heads[0], &heads[1]) {
                (Some((a, _)), Some((b, _))) => {
                    let a = a["seq"].as_u64().unwrap();
                    let b = b["seq"].as_u64().unwrap();
                    if a == b {
                        return Err(ServiceError::Invalid(
                            "durable and audit events have duplicate seq".into(),
                        ));
                    }
                    usize::from(b < a)
                }
                (Some(_), None) => 0,
                _ => 1,
            };
            let (value, size) = heads[index].take().unwrap();
            if !events.is_empty()
                && (events.len() >= EVENT_BATCH_COUNT || bytes + size > EVENT_BATCH_BYTES)
            {
                heads[index] = Some((value, size));
                break;
            }
            cursor.offsets[index] += size as u64;
            cursor.counts[index] += 1;
            if cursor.counts[index]
                > if index == 0 {
                    limits
                        .max_event_count
                        .unwrap_or(policy::DEFAULT_MAX_JOURNAL_EVENT_COUNT)
                } else {
                    policy::DEFAULT_MAX_JOURNAL_EVENT_COUNT
                }
            {
                return Err(ServiceError::Invalid(
                    "journal exceeds event count limit".into(),
                ));
            }
            let seq = value["seq"].as_u64().unwrap();
            if seq <= cursor.stream_seq[index] {
                return Err(ServiceError::Invalid(
                    "journal seq is not strictly increasing".into(),
                ));
            }
            cursor.stream_seq[index] = seq;
            if seq > cursor.last_seq {
                cursor.last_seq = seq;
                bytes += size;
                events.push(value);
            }
            heads[index] = next(&mut readers[index], max_event[index])?;
        }
        Ok(EventBatch {
            events,
            cursor,
            exhausted: heads.iter().all(Option::is_none),
        })
    }
}
impl LocalService {
    /// Observation export enumerates directories without folding each run.
    /// A damaged journal is handled independently by read_event_batch.
    pub async fn observable_run_ids(&self) -> Result<Vec<String>, ApiError> {
        let root = self.inner.runs_dir.clone();
        let cap = self.inner.deployment_policy.max_directory_scan_entries;
        self.blocking_read(move || {
            let mut ids = Vec::new();
            let mut scanned = 0;
            if !root.exists() {
                return Ok(ids);
            }
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if crate::run_dirs::is_store_coordination_name(&name) {
                    continue;
                }
                scanned += 1;
                if scanned > cap {
                    return Err(ServiceError::Invalid(
                        "run directory scan limit exceeded".into(),
                    ));
                }
                if entry.file_type()?.is_dir() && policy::is_safe_path_component(&name) {
                    ids.push(name);
                }
            }
            ids.sort();
            Ok(ids)
        })
        .await
    }
    pub(crate) async fn prepare_replay(
        &self,
        directory: camino::Utf8PathBuf,
        after: u64,
    ) -> Result<(u64, u64, bool), ApiError> {
        let view = self.read_view(directory.clone()).await?;
        let settled = view.state.terminal.is_some();
        let last = self
            .blocking_read(move || {
                let meta = run_meta_dir(&directory);
                let limits = engine::JournalLimits::default();
                let durable =
                    engine::read_last_seq_from_file_tail(&meta.join("journal.jsonl"), limits)
                        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
                let audit = engine::read_last_seq_from_file_tail(&meta.join("audit.jsonl"), limits)
                    .map_err(|error| ServiceError::Invalid(error.to_string()))?;
                Ok(durable.max(audit))
            })
            .await?;
        Ok((last, if after > last { 0 } else { after }, settled))
    }
    /// Bounded history-only replay, including during server drain. No live
    /// poller or background writer is started.
    pub async fn history_events(
        &self,
        id: &str,
        after: u64,
    ) -> Result<
        (
            futures_util::stream::BoxStream<'static, RunEvent>,
            bool,
            u64,
        ),
        ApiError,
    > {
        let directory = self.run_dir_for(id).await?;
        let (last, cursor, settled) = self.prepare_replay(directory.clone(), after).await?;
        Ok((
            self.replay_stream(
                directory,
                cursor,
                last,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(settled)),
            ),
            settled,
            last,
        ))
    }
    pub(crate) fn replay_stream(
        &self,
        directory: camino::Utf8PathBuf,
        after: u64,
        through: u64,
        settled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> futures_util::stream::BoxStream<'static, RunEvent> {
        use futures_util::StreamExt;
        let service = self.clone();
        let id = directory.file_name().unwrap_or("unknown").to_string();
        futures_util::stream::unfold(
            (
                EventCursor::default(),
                std::collections::VecDeque::new(),
                false,
                after,
            ),
            move |(mut cursor, mut pending, mut done, mut delivered)| {
                let service = service.clone();
                let directory = directory.clone();
                let id = id.clone();
                let settled = settled.clone();
                async move {
                    loop {
                        if let Some(event) = pending.pop_front() {
                            let event: RunEvent = event;
                            if api::is_terminal_event_kind(&event.kind) {
                                settled.store(true, std::sync::atomic::Ordering::Relaxed);
                            } else if matches!(
                                event.kind.as_str(),
                                "run_queued" | "run_started" | "run_resumed"
                            ) {
                                settled.store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                            delivered = event.seq;
                            return Some((event, (cursor, pending, done, delivered)));
                        }
                        if done {
                            return None;
                        }
                        let current = cursor.clone();
                        let path = directory.clone();
                        match service.blocking_read(move || current.read(&path)).await {
                            Ok(batch) => {
                                done = batch.exhausted
                                    || batch.events.iter().any(|event| {
                                        event["seq"].as_u64().is_some_and(|seq| seq > through)
                                    });
                                cursor = batch.cursor;
                                for value in batch.events {
                                    let seq = value["seq"].as_u64().unwrap();
                                    if seq > after && seq <= through {
                                        pending.push_back(
                                            RunEvent::from_flat(&value)
                                                .expect("batch already type-checked"),
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                done = true;
                                settled.store(true, std::sync::atomic::Ordering::Relaxed);
                                pending.push_back(crate::summaries::stream_error_event(
                                    &id,
                                    delivered,
                                    error.to_string(),
                                ));
                            }
                        }
                    }
                }
            },
        )
        .boxed()
    }

    pub async fn read_event_batch(
        &self,
        id: &str,
        cursor: EventCursor,
    ) -> Result<EventBatch, ApiError> {
        let directory = self.run_dir_for(id).await?;
        self.blocking_read(move || cursor.read(&directory)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    fn record(seq: usize) -> Value {
        serde_json::json!({"t":"run_started","seq":seq,"ts":"2026-10-02T00:00:00Z","run_id":"test","trace_id":api::trace_id_for_run("test"),"span_id":api::span_id_for_seq(seq as u64),"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1})
    }
    #[test]
    fn merged_batches_are_bounded_and_incomplete_lines_wait() {
        let directory = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-batch-{}", uuid::Uuid::now_v7()));
        let meta = run_meta_dir(&directory);
        std::fs::create_dir_all(&meta).unwrap();
        let mut durable = String::new();
        let mut audit = String::new();
        for seq in 1..=600 {
            let line = format!("{}\n", record(seq));
            if seq % 2 == 0 {
                audit.push_str(&line);
            } else {
                durable.push_str(&line);
            }
        }
        std::fs::write(meta.join("journal.jsonl"), durable).unwrap();
        std::fs::write(meta.join("audit.jsonl"), audit).unwrap();
        let mut cursor = EventCursor::default();
        let mut seen = Vec::new();
        loop {
            let batch = cursor.read(&directory).unwrap();
            assert!(batch.events.len() <= EVENT_BATCH_COUNT);
            seen.extend(batch.events.iter().map(|e| e["seq"].as_u64().unwrap()));
            cursor = batch.cursor;
            if batch.exhausted {
                break;
            }
        }
        assert_eq!(seen, (1..=600).collect::<Vec<_>>());
        assert!(cursor.read(&directory).unwrap().events.is_empty());
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(meta.join("journal.jsonl"))
            .unwrap();
        write!(file, "{}", record(601)).unwrap();
        assert!(cursor.read(&directory).unwrap().events.is_empty());
        writeln!(file).unwrap();
        assert_eq!(cursor.read(&directory).unwrap().events[0]["seq"], 601);
        std::fs::write(
            meta.join("audit.jsonl"),
            vec![b'x'; policy::DEFAULT_MAX_JOURNAL_EVENT_BYTES + 2],
        )
        .unwrap();
        assert!(EventCursor::default().read(&directory).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
