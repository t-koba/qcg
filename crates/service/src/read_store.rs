//! Rebuildable read views. Execution, approvals and integrity checks read originals.
use crate::{LocalService, ServiceError, run_meta_dir};
use api::{ApiError, RunEvent};
use camino::{Utf8Path, Utf8PathBuf};
use engine::RunState;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read, Seek},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime},
};

const READ_CONCURRENCY: usize = 8;

// Upper-bound accounting includes collection storage and string capacities,
// rather than assuming a fixed multiple of the serialized JSON length.
fn value_memory(value: &Value) -> usize {
    let children = match value {
        Value::String(value) => value.capacity(),
        Value::Array(values) => {
            values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>())
                + values.iter().map(value_memory).sum::<usize>()
        }
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                key.capacity()
                    + value_memory(value)
                    + 2 * (std::mem::size_of::<String>()
                        + std::mem::size_of::<Value>()
                        + 8 * std::mem::size_of::<usize>())
            })
            .sum(),
        _ => 0,
    };
    std::mem::size_of::<Value>().saturating_add(children)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Revision {
    len: u64,
    modified: SystemTime,
    identity: (u64, u64),
}
impl Revision {
    fn of(file: &File) -> std::io::Result<Self> {
        let meta = file.metadata()?;
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            (meta.dev(), meta.ino())
        };
        #[cfg(not(unix))]
        let identity = (0, 0);
        Ok(Self {
            len: meta.len(),
            modified: meta.modified()?,
            identity,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RunReadView {
    count: usize,
    pub last_started: Option<u64>,
    pub last_queued: Option<u64>,
    pub events: Vec<RunEvent>,
    pub state: RunState,
    offset: u64,
}
#[derive(Debug)]
struct Entry {
    revision: Revision,
    tail: Vec<u8>,
    view: Arc<RunReadView>,
    bytes: usize,
    used: Instant,
}
#[derive(Debug, Default)]
struct Cache {
    views: BTreeMap<Utf8PathBuf, Entry>,
    recency: BTreeMap<(Instant, Utf8PathBuf), ()>,
    bytes: usize,
}
impl Cache {
    fn remove(&mut self, path: &Utf8Path) -> Option<Entry> {
        let entry = self.views.remove(path)?;
        self.recency.remove(&(entry.used, path.to_owned()));
        self.bytes -= entry.bytes;
        Some(entry)
    }
}
#[derive(Debug)]
pub(crate) struct ReadStore {
    entries: Mutex<Cache>,
    shards: [Mutex<()>; 64],
    max_entries: usize,
    max_bytes: usize,
    pub permits: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    pub bytes_read: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub applied: std::sync::atomic::AtomicU64,
}
impl ReadStore {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: Mutex::new(Cache::default()),
            shards: std::array::from_fn(|_| Mutex::new(())),
            max_entries,
            max_bytes,
            permits: Arc::new(tokio::sync::Semaphore::new(READ_CONCURRENCY)),
            #[cfg(test)]
            bytes_read: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            applied: std::sync::atomic::AtomicU64::new(0),
        }
    }
    pub fn read(&self, directory: &Utf8Path) -> Result<Arc<RunReadView>, ServiceError> {
        use std::hash::{Hash, Hasher};
        let path = run_meta_dir(directory).join("journal.jsonl");
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hash);
        let _flight = self.shards[hash.finish() as usize % self.shards.len()]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut file = File::open(&path)?;
        let revision = Revision::of(&file)?;
        let limits = engine::JournalLimits::default();
        if revision.len > limits.max_total_bytes.unwrap_or(usize::MAX) as u64 {
            return Err(ServiceError::Invalid(
                "run journal exceeds byte limit".into(),
            ));
        }
        let prior = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = entries.views.get_mut(&path)
                && entry.revision == revision
            {
                let old = entry.used;
                let used = Instant::now();
                entry.used = used;
                let view = entry.view.clone();
                entries.recency.remove(&(old, path.clone()));
                entries.recency.insert((used, path.clone()), ());
                return Ok(view);
            }
            entries.remove(&path)
        };
        // Unix identifies replacement by inode. Other platforms rebuild on any
        // change rather than guessing whether a replacement was an append.
        let mut view = RunReadView::default();
        if let Some(entry) = prior
            && cfg!(unix)
            && entry.revision.identity == revision.identity
            && revision.len > entry.revision.len
        {
            // Truncate-and-requeue can grow past the previous length. Verify
            // the old tail before treating the change as an append.
            file.seek(std::io::SeekFrom::Start(
                entry.revision.len.saturating_sub(entry.tail.len() as u64),
            ))?;
            let mut tail = vec![0; entry.tail.len()];
            file.read_exact(&mut tail)?;
            #[cfg(test)]
            self.bytes_read
                .fetch_add(tail.len() as u64, std::sync::atomic::Ordering::Relaxed);
            if tail == entry.tail {
                view = Arc::unwrap_or_clone(entry.view);
            }
        }
        file.seek(std::io::SeekFrom::Start(view.offset))?;
        let mut reader = BufReader::new(file.take(revision.len - view.offset));
        loop {
            let mut line = Vec::new();
            let count = (&mut reader)
                .take(
                    limits
                        .max_event_bytes
                        .unwrap_or(usize::MAX)
                        .saturating_add(2) as u64,
                )
                .read_until(b'\n', &mut line)?;
            #[cfg(test)]
            self.bytes_read
                .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
            if count == 0 {
                break;
            }
            if count
                > limits
                    .max_event_bytes
                    .unwrap_or(usize::MAX)
                    .saturating_add(1)
            {
                return Err(ServiceError::Invalid(
                    "run journal event exceeds byte limit".into(),
                ));
            }
            if line.last() != Some(&b'\n') {
                break;
            }
            if view.count >= limits.max_event_count.unwrap_or(usize::MAX) {
                return Err(ServiceError::Invalid(
                    "run journal exceeds event count limit".into(),
                ));
            }
            let value: Value = serde_json::from_slice(&line)?;
            let event = RunEvent::from_flat(&value).map_err(ServiceError::Invalid)?;
            view.state
                .apply(&value)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?;
            #[cfg(test)]
            self.applied
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            view.offset += count as u64;
            view.count += 1;
            match event.kind.as_str() {
                "run_started" => view.last_started = Some(event.seq),
                "run_queued" => view.last_queued = Some(event.seq),
                _ => {}
            }
            if matches!(
                event.kind.as_str(),
                "run_started" | "run_queued" | "run_resumed"
            ) || api::is_terminal_event_kind(&event.kind)
            {
                view.events
                    .retain(|prior| !api::is_terminal_event_kind(&prior.kind));
            }
            let identity = matches!(event.kind.as_str(), "run_started" | "run_queued");
            if identity
                && view
                    .events
                    .iter()
                    .any(|prior| matches!(prior.kind.as_str(), "run_started" | "run_queued"))
            {
                continue;
            }
            let unpriced = match &event.data {
                api::RunEventData::LlmCall(call) if call.tokens.input.saturating_add(call.tokens.output)>0 && call.cost_microusd==0 => !view.events.iter().any(|prior| matches!(&prior.data,api::RunEventData::LlmCall(old) if old.provider==call.provider && old.model==call.model)),
                _ => false,
            };
            if identity || api::is_terminal_event_kind(&event.kind) || unpriced {
                view.events.push(event);
            }
        }
        let mut file = reader.into_inner().into_inner();
        let tail_len = revision.len.min(4096);
        file.seek(std::io::SeekFrom::Start(revision.len - tail_len))?;
        let mut tail = vec![0; tail_len as usize];
        file.read_exact(&mut tail)?;
        #[cfg(test)]
        self.bytes_read
            .fetch_add(tail.len() as u64, std::sync::atomic::Ordering::Relaxed);
        // Large views are returned without retention; parser buffers and
        // original journal sizes have separate bounds.
        let state_bytes = serde_json::to_vec(&view.state)?.len();
        if state_bytes > policy::DEFAULT_MAX_STATE_BYTES {
            return Err(ServiceError::Invalid("run state exceeds byte limit".into()));
        }
        let bytes = value_memory(&serde_json::to_value(&view.state)?)
            + value_memory(&serde_json::to_value(&view.events)?)
            + tail.capacity()
            + 2 * path.as_str().len()
            + std::mem::size_of::<RunReadView>()
            + std::mem::size_of::<Entry>()
            + 256;
        let view = Arc::new(view);
        if bytes <= self.max_bytes && view.offset == revision.len {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !entries.views.is_empty()
                && (entries.views.len() >= self.max_entries
                    || entries.bytes.saturating_add(bytes) > self.max_bytes)
            {
                let oldest = entries
                    .recency
                    .keys()
                    .next()
                    .map(|(_, path)| path.clone())
                    .unwrap();
                entries.remove(&oldest);
            }
            if self.max_entries > 0 {
                let used = Instant::now();
                entries.bytes += bytes;
                entries.recency.insert((used, path.clone()), ());
                entries.views.insert(
                    path,
                    Entry {
                        revision,
                        tail,
                        view: view.clone(),
                        bytes,
                        used,
                    },
                );
            }
        }
        Ok(view)
    }
}

impl LocalService {
    /// Safety decisions verify originals, with the same blocking-work bound
    /// as observation reads. Callers retain their execution/journal authority.
    pub(crate) async fn authoritative_state(
        &self,
        directory: &Utf8Path,
    ) -> Result<RunState, ApiError> {
        let directory = directory.to_owned();
        self.blocking_read(move || crate::summaries::fold_run_state(&directory))
            .await
    }

    pub(crate) async fn blocking_work<T: Send + 'static>(
        &self,
        read: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
    ) -> Result<T, ServiceError> {
        let permit = self
            .inner
            .read_store
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| ServiceError::Invalid(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            read()
        })
        .await
        .map_err(|e| ServiceError::Invalid(e.to_string()))?
    }

    pub(crate) async fn blocking_read<T: Send + 'static>(
        &self,
        read: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
    ) -> Result<T, ApiError> {
        self.blocking_work(read).await.map_err(ApiError::from)
    }
    pub(crate) async fn read_view(
        &self,
        directory: Utf8PathBuf,
    ) -> Result<Arc<RunReadView>, ApiError> {
        let store = self.inner.read_store.clone();
        self.blocking_read(move || store.read(&directory)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    fn journal() -> (Utf8PathBuf, engine::JournalWriter) {
        let directory = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-read-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(run_meta_dir(&directory)).unwrap();
        let writer = engine::JournalWriter::create(
            &run_meta_dir(&directory).join("journal.jsonl"),
            "test",
            false,
            None,
        )
        .unwrap();
        writer
            .event("run_started", serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1}))
            .unwrap();
        (directory, writer)
    }
    #[test]
    fn concurrent_reads_coalesce_and_append_applies_only_new_events() {
        let (directory, writer) = journal();
        let store = Arc::new(ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES));
        std::thread::scope(|scope| {
            for _ in 0..64 {
                let store = store.clone();
                let directory = directory.clone();
                scope.spawn(move || assert_eq!(store.read(&directory).unwrap().state.last_seq, 1));
            }
        });
        assert_eq!(store.applied.load(Ordering::Relaxed), 1);
        let read = store.bytes_read.load(Ordering::Relaxed);
        store.read(&directory).unwrap();
        assert_eq!(store.bytes_read.load(Ordering::Relaxed), read);
        writer
            .event("run_started", serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1}))
            .unwrap();
        assert_eq!(store.read(&directory).unwrap().state.last_seq, 2);
        // Non-Unix rebuilds the view on any change instead of guessing
        // appends (see `ReadStore::read`): the first read applied 1 event
        // and this rebuild applies 2, while Unix reuses the view and
        // applies only the 1 new event.
        #[cfg(unix)]
        assert_eq!(store.applied.load(Ordering::Relaxed), 2);
        #[cfg(not(unix))]
        assert_eq!(store.applied.load(Ordering::Relaxed), 3);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn replacement_truncation_corruption_and_entry_eviction() {
        let (directory, writer) = journal();
        let store = ReadStore::new(1, policy::DEFAULT_READ_CACHE_BYTES);
        store.read(&directory).unwrap();
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let original = std::fs::read(&path).unwrap();
        writer
            .event("run_started", serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1}))
            .unwrap();
        store.read(&directory).unwrap();
        std::fs::write(&path, &original).unwrap();
        assert_eq!(store.read(&directory).unwrap().state.last_seq, 1);
        let replacement = path.with_extension("replacement");
        std::fs::write(&replacement, &original).unwrap();
        std::fs::rename(replacement, &path).unwrap();
        assert_eq!(store.read(&directory).unwrap().state.last_seq, 1);
        std::fs::write(&path, b"not json\n").unwrap();
        assert!(store.read(&directory).is_err());
        let (other, _) = journal();
        store.read(&other).unwrap();
        assert!(store.entries.lock().unwrap().views.len() <= 1);
        assert!(
            store
                .entries
                .lock()
                .unwrap()
                .views
                .values()
                .map(|e| e.bytes)
                .sum::<usize>()
                <= store.max_bytes
        );
        std::fs::remove_dir_all(directory).unwrap();
        std::fs::remove_dir_all(other).unwrap();
    }
}

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::sync::atomic::Ordering;
    fn percentile(mut values: Vec<f64>, percentile: f64) -> f64 {
        values.sort_by(f64::total_cmp);
        values[((values.len() - 1) as f64 * percentile).ceil() as usize]
    }
    #[test]
    #[ignore = "performance matrix; run explicitly with --ignored --exact"]
    fn read_view_performance_matrix() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-benchmark-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let mut rows = Vec::new();
        for runs in [100, 1000, 10000] {
            let directories = (0..runs)
                .map(|index| root.join(format!("run-{index}")))
                .collect::<Vec<_>>();
            for directory in &directories {
                std::fs::create_dir_all(run_meta_dir(directory)).unwrap();
                let writer = engine::JournalWriter::create(
                    &run_meta_dir(directory).join("journal.jsonl"),
                    "test",
                    false,
                    None,
                )
                .unwrap();
                writer.event("run_started",serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1})).unwrap();
            }
            for journal_mib in [1, 16, 64] {
                let path = run_meta_dir(&directories[0]).join("journal.jsonl");
                let identity = std::fs::read(&path).unwrap();
                // Keep the first identity; repeated runs of this case reset the
                // large target rather than accumulating prior measurements.
                let first = identity
                    .split_inclusive(|byte| *byte == b'\n')
                    .next()
                    .unwrap();
                let mut contents = first.to_vec();
                let mut seq = 2;
                let padding = "x".repeat(512 * 1024);
                loop {
                    let line = format!(
                        "{}\n",
                        serde_json::json!({"t":"budget_charged","seq":seq,"ts":"2026-10-02T00:00:00Z","run_id":"test","trace_id":api::trace_id_for_run("test"),"span_id":api::span_id_for_seq(seq),"amount":0,"node":"bench","padding":padding})
                    );
                    let remaining = journal_mib * 1024 * 1024 - contents.len();
                    if line.len() > remaining {
                        let overhead = line.len() - padding.len();
                        if remaining >= overhead {
                            let mut value: Value = serde_json::from_str(line.trim_end()).unwrap();
                            value["padding"] = Value::String("x".repeat(remaining - overhead));
                            let last = format!("{}\n", value);
                            assert_eq!(last.len(), remaining);
                            contents.extend_from_slice(last.as_bytes());
                            seq += 1;
                        }
                        break;
                    }
                    contents.extend_from_slice(line.as_bytes());
                    seq += 1;
                }

                std::fs::write(&path, &contents).unwrap();
                drop(contents);
                drop(identity);
                for readers in [1, 8, 64] {
                    let store = Arc::new(ReadStore::new(10000, 256 * 1024 * 1024));
                    let census = Instant::now();
                    for directory in directories.iter().skip(1) {
                        store.read(directory).unwrap();
                    }
                    let census_ms = census.elapsed().as_secs_f64() * 1000.0;
                    let baseline_bytes = store.bytes_read.load(Ordering::Relaxed);
                    let baseline_folds = store.applied.load(Ordering::Relaxed);
                    let repeated_census = Instant::now();
                    for directory in directories.iter().skip(1) {
                        store.read(directory).unwrap();
                    }
                    let warm_census_ms = repeated_census.elapsed().as_secs_f64() * 1000.0;
                    assert_eq!(
                        store.bytes_read.load(Ordering::Relaxed),
                        baseline_bytes,
                        "a fitting unchanged census must not reread journals"
                    );
                    assert_eq!(
                        store.applied.load(Ordering::Relaxed),
                        baseline_folds,
                        "a fitting unchanged census must not refold journals"
                    );
                    let mut cold = Vec::new();
                    let mut warm = Vec::new();
                    for iteration in 0..21 {
                        let barrier = Arc::new(std::sync::Barrier::new(readers));
                        let measurements = std::thread::scope(|scope| {
                            let handles = (0..readers)
                                .map(|_| {
                                    let store = store.clone();
                                    let directory = directories[0].clone();
                                    let barrier = barrier.clone();
                                    scope.spawn(move || {
                                        barrier.wait();
                                        let start = Instant::now();
                                        store.read(&directory).unwrap();
                                        start.elapsed().as_secs_f64() * 1000.0
                                    })
                                })
                                .collect::<Vec<_>>();
                            handles
                                .into_iter()
                                .map(|handle| handle.join().unwrap())
                                .collect::<Vec<_>>()
                        });
                        if iteration == 0 {
                            cold.extend(measurements);
                        } else {
                            warm.extend(measurements);
                        }
                    }
                    let after_bytes = store.bytes_read.load(Ordering::Relaxed);
                    let after_folds = store.applied.load(Ordering::Relaxed);
                    assert_eq!(
                        after_folds - baseline_folds,
                        seq - 1,
                        "single-flight must fold each target event only once"
                    );
                    let entries = store.entries.lock().unwrap();
                    let charged = entries
                        .views
                        .values()
                        .map(|entry| entry.bytes)
                        .sum::<usize>();
                    assert_eq!(charged, entries.bytes);
                    assert_eq!(entries.views.len(), entries.recency.len());
                    assert!(charged <= store.max_bytes);
                    assert!(entries.views.len() <= 10000);
                    #[cfg(unix)]
                    let usage = {
                        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
                        unsafe {
                            libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
                            usage.assume_init()
                        }
                    };
                    #[cfg(unix)]
                    let (rss, user, system) = (
                        usage.ru_maxrss,
                        usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6,
                        usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6,
                    );
                    #[cfg(not(unix))]
                    let (rss, user, system) = (0, 0.0, 0.0);
                    rows.push(serde_json::json!({"runs":runs,"readers":readers,"journal_mib":journal_mib,"journal_bytes":std::fs::metadata(&path).unwrap().len(),"census_ms":census_ms,"warm_census_ms":warm_census_ms,"cache_limit_bytes":store.max_bytes,"cold_p50_ms":percentile(cold.clone(),0.5),"cold_p95_ms":percentile(cold,0.95),"warm_p50_ms":percentile(warm.clone(),0.5),"warm_p95_ms":percentile(warm,0.95),"read_bytes":after_bytes-baseline_bytes,"fold_events":after_folds-baseline_folds,"retained_bytes_charged":charged,
                        "process_max_rss_native":rss,"process_user_cpu_seconds":user,"process_system_cpu_seconds":system
                    }));
                }
            }
            std::fs::remove_dir_all(&root).unwrap();
            std::fs::create_dir_all(&root).unwrap();
        }
        let path = std::env::var("QCG_BENCHMARK_REPORT")
            .unwrap_or_else(|_| "/tmp/qcg-read-benchmark.json".into());
        let path = if std::path::Path::new(&path).is_absolute() {
            std::path::PathBuf::from(path)
        } else {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(path)
        };
        std::fs::write(path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
