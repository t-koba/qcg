//! Rebuildable read views. Execution, approvals and integrity checks read originals.
tokio::task_local! {
    static ACCEPTANCE_GUARD: std::sync::Arc<dyn Send + Sync>;
}
/// Retain a caller-owned acceptance guard through service work, including
/// blocking writers that keep running after cancellation of the calling future.
pub async fn with_acceptance_guard<T>(
    guard: std::sync::Arc<dyn Send + Sync>,
    work: impl std::future::Future<Output = T>,
) -> T {
    ACCEPTANCE_GUARD.scope(guard, work).await
}

use crate::summaries::{run_identity_event, status_from_journal, status_name_from_state};
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
#[derive(Debug, Clone)]
enum CachedView {
    Full(Arc<RunReadView>),
    List(api::RunListItem),
}
impl CachedView {
    fn list(&self, directory: &Utf8Path) -> Result<api::RunListItem, ServiceError> {
        let view = match self {
            Self::Full(view) => view,
            Self::List(item) => return Ok(item.clone()),
        };
        let (identity, started) = run_identity_event(directory, &view.events)?;
        let generator = started.generator.split_once('@').map(|(id, _)| id);
        Ok(api::RunListItem {
            invalidation: None,
            run_id: directory.file_name().unwrap().to_owned(),
            state: status_from_journal(status_name_from_state(&view.state))?,
            generator_id: generator.unwrap_or(&started.generator).to_owned(),
            started_at: identity.ts.clone(),
            seq: view.state.last_seq,
        })
    }
}
#[derive(Debug)]
struct Entry {
    revision: Revision,
    tail: Vec<u8>,
    view: CachedView,
    bytes: usize,
    used: Instant,
}
#[derive(Debug, Default)]
struct Cache {
    views: BTreeMap<Utf8PathBuf, Entry>,
    recency: BTreeMap<(Instant, Utf8PathBuf), ()>,
    bytes: usize,
    full_count: usize,
}
impl Cache {
    fn remove(&mut self, path: &Utf8Path) -> Option<Entry> {
        let entry = self.views.remove(path)?;
        self.recency.remove(&(entry.used, path.to_owned()));
        self.bytes -= entry.bytes;
        self.full_count -= usize::from(matches!(entry.view, CachedView::Full(_)));
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
    fn lock(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
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
        match self.read_cached(directory, false)? {
            CachedView::Full(view) => Ok(view),
            CachedView::List(_) => unreachable!(),
        }
    }
    pub fn list_item(&self, directory: &Utf8Path) -> Result<api::RunListItem, ServiceError> {
        let mut item = self.read_cached(directory, true)?.list(directory)?;
        item.invalidation = crate::invalidation::read_invalidation(directory)?;
        Ok(item)
    }
    // Prune only after a complete successful census; concurrent readers lose at most a reusable entry.
    pub fn prune(&self, root: &Utf8Path, items: &[api::RunListItem]) {
        let live: std::collections::BTreeSet<&str> =
            items.iter().map(|item| item.run_id.as_str()).collect();
        let mut entries = self.lock();
        let dead: Vec<_> = entries
            .views
            .keys()
            .filter(|path| {
                let dir = path.parent().unwrap().parent().unwrap();
                dir.parent() == Some(root) && !live.contains(dir.file_name().unwrap())
            })
            .cloned()
            .collect();
        for path in dead {
            entries.remove(&path);
        }
    }
    fn read_cached(&self, directory: &Utf8Path, list: bool) -> Result<CachedView, ServiceError> {
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
            let mut entries = self.lock();
            if let Some(entry) = entries.views.get(&path)
                && entry.revision == revision
                && (list || matches!(entry.view, CachedView::Full(_)))
            {
                if list && matches!(entry.view, CachedView::Full(_)) {
                    let old_bytes = entry.bytes;
                    let item = entry.view.list(directory)?;
                    let bytes = item.run_id.capacity()
                        + item.generator_id.capacity()
                        + item.started_at.capacity()
                        + 2 * path.as_str().len()
                        + std::mem::size_of::<Entry>()
                        + 256;
                    entries.bytes = entries.bytes - old_bytes + bytes;
                    entries.full_count -= 1;
                    let entry = entries.views.get_mut(&path).unwrap();
                    entry.bytes = bytes;
                    entry.tail = Vec::new();
                    entry.view = CachedView::List(item);
                }
                let entry = entries.views.get_mut(&path).unwrap();
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
            && let CachedView::Full(prior_view) = entry.view
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
                view = Arc::unwrap_or_clone(prior_view);
            }
        }
        file.seek(std::io::SeekFrom::Start(view.offset))?;
        let mut reader = BufReader::new(file.take(revision.len - view.offset));
        let mut terminated = true;
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
            let has_newline = line.last() == Some(&b'\n');
            let body_len = line.len() - usize::from(has_newline);
            if body_len > limits.max_event_bytes.unwrap_or(usize::MAX) {
                return Err(ServiceError::Invalid(
                    "run journal event exceeds byte limit".into(),
                ));
            }
            // Even a complete JSON value or whitespace tail can change meaning
            // after an append. Only LF-terminated views are safe to cache.
            terminated = has_newline;
            let body = &line[..body_len];
            if body.iter().all(u8::is_ascii_whitespace) {
                view.offset += count as u64;
                continue;
            }
            if view.count >= limits.max_event_count.unwrap_or(usize::MAX) {
                return Err(ServiceError::Invalid(
                    "run journal exceeds event count limit".into(),
                ));
            }
            let value: Value = match serde_json::from_slice(body) {
                Ok(value) => value,
                Err(error) if !has_newline && error.is_eof() && reader.fill_buf()?.is_empty() => {
                    break;
                }
                Err(error) => return Err(error.into()),
            };
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
        let tail_len = if list { 0 } else { revision.len.min(4096) };
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
        let complete = terminated && view.offset == revision.len;
        let view = CachedView::Full(Arc::new(view));
        let (view, payload_bytes) = if list {
            let item = view.list(directory)?;
            let bytes =
                item.run_id.capacity() + item.generator_id.capacity() + item.started_at.capacity();
            (CachedView::List(item), bytes)
        } else {
            let CachedView::Full(full) = &view else {
                unreachable!()
            };
            let bytes = value_memory(&serde_json::to_value(&full.state)?)
                + value_memory(&serde_json::to_value(&full.events)?)
                + std::mem::size_of::<RunReadView>();
            (view, bytes)
        };
        // Both variants share this charge and LRU. Compact entry count is
        // bounded by the byte budget, not the active-run tracking limit.
        let bytes = payload_bytes
            + tail.capacity()
            + 2 * path.as_str().len()
            + std::mem::size_of::<Entry>()
            + 256;
        if complete && bytes <= self.max_bytes && (list || self.max_entries > 0) {
            let mut entries = self.lock();
            while !entries.views.is_empty()
                && ((!list && entries.full_count >= self.max_entries)
                    || entries.bytes.saturating_add(bytes) > self.max_bytes)
            {
                let ((_, oldest), _) = entries.recency.pop_first().unwrap();
                entries.remove(&oldest);
            }
            let used = Instant::now();
            entries.bytes += bytes;
            entries.full_count += usize::from(!list);
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
        #[cfg(test)]
        crate::tests::cancel_observation::fail_observation(directory, "fold")
            .map_err(ApiError::from)?;
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
        let guard = ACCEPTANCE_GUARD.try_with(Clone::clone).ok();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
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
    #[tokio::test]
    async fn cancelled_acceptance_retains_guard_until_blocking_writer_exits() {
        struct Guard(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let root = crate::tests::support::TempGuard(crate::tests::support::temp_run_dir(
            "acceptance-guard",
        ));
        let service = crate::tests::support::test_service(vec![], root.0.clone());
        let (dropped, mut observed_drop) = tokio::sync::oneshot::channel();
        let (entered, observed_entry) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let task = tokio::spawn(with_acceptance_guard(
            Arc::new(Guard(Some(dropped))),
            async move {
                service
                    .blocking_work(move || {
                        entered.send(()).unwrap();
                        blocked.recv().unwrap();
                        Ok(())
                    })
                    .await
            },
        ));
        observed_entry.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(matches!(
            observed_drop.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), observed_drop)
            .await
            .unwrap()
            .unwrap();
    }

    fn journal() -> (Utf8PathBuf, engine::JournalWriter) {
        let directory = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-read-{}", uuid::Uuid::now_v7()));
        journal_at(directory)
    }
    fn journal_at(directory: Utf8PathBuf) -> (Utf8PathBuf, engine::JournalWriter) {
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
    fn census(root: &Utf8Path, store: &ReadStore) -> Vec<api::RunListItem> {
        crate::summaries::list_items_cached(root, 100, store).unwrap()
    }
    #[test]
    fn compact_census_coalesces_beyond_full_count_limit_and_refreshes_sidecars() {
        let root =
            crate::tests::support::TempGuard(crate::tests::support::temp_run_dir("list-cache"));
        let root = root.0.as_path();
        let (run, writer) = journal_at(root.join("first"));
        journal_at(root.join("second"));
        let store = ReadStore::new(1, policy::DEFAULT_READ_CACHE_BYTES);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                scope.spawn(|| assert_eq!(census(root, &store).len(), 2));
            }
        });
        assert_eq!(store.applied.load(Ordering::Relaxed), 2);
        let bytes = store.bytes_read.load(Ordering::Relaxed);
        let before = serde_json::to_value(census(root, &store)).unwrap();
        assert_eq!(store.bytes_read.load(Ordering::Relaxed), bytes);
        assert_eq!(store.entries.lock().unwrap().views.len(), 2);
        let record = serde_json::json!({"run_id":"first", "reason":"obsolete", "actor_label":"test", "invalidated_at":"2026-01-01T00:00:00Z"});
        let sidecar = run_meta_dir(&run).join("invalidation.json");
        std::fs::write(&sidecar, serde_json::to_vec(&record).unwrap()).unwrap();
        let after = serde_json::to_value(census(root, &store)).unwrap();
        assert_ne!(before, after);
        assert_eq!(store.applied.load(Ordering::Relaxed), 2);
        std::fs::write(&sidecar, b"broken").unwrap();
        assert!(crate::summaries::list_items_cached(root, 100, &store).is_err());
        std::fs::remove_file(&sidecar).unwrap();
        // A compact hit must not substitute for a full detail read.
        assert_eq!(store.read(&run).unwrap().state.last_seq, 1);
        assert_eq!(store.applied.load(Ordering::Relaxed), 3);
        drop(writer);
    }
    #[test]
    fn compact_revisions_rebuild_and_corruption_never_falls_back() {
        let root =
            crate::tests::support::TempGuard(crate::tests::support::temp_run_dir("list-cache"));
        let root = root.0.as_path();
        let (run, writer) = journal_at(root.join("first"));
        journal_at(root.join("off-page"));
        let store = ReadStore::new(1, policy::DEFAULT_READ_CACHE_BYTES);
        census(root, &store);
        let path = run_meta_dir(&run).join("journal.jsonl");
        let original = std::fs::read(&path).unwrap();
        writer
            .event("run_finished", serde_json::json!({"status":"success"}))
            .unwrap();
        assert_eq!(store.list_item(&run).unwrap().seq, 2);
        assert_eq!(
            store.list_item(&run).unwrap().state,
            api::RunStatus::Succeeded
        );
        drop(writer);
        // Replacement uses a new inode; truncation uses the existing inode.
        let replacement = path.with_extension("replacement");
        std::fs::write(&replacement, &original).unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert_eq!(store.list_item(&run).unwrap().seq, 1);
        std::fs::write(&path, b"not json\n").unwrap();
        assert!(store.list_item(&run).is_err());
        std::fs::write(&path, &original).unwrap();
        assert_eq!(store.list_item(&run).unwrap().seq, 1);
        let off_page = root.join("off-page/meta/journal.jsonl");
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&off_page)
            .unwrap()
            .write_all(b"broken\n")
            .unwrap();
        // Every candidate is validated before any HTTP filter/page selection.
        assert!(crate::summaries::list_items_cached(root, 100, &store).is_err());
        std::fs::write(&path, []).unwrap();
        // Empty/missing journals are excluded and pruned on a successful scan.
        std::fs::remove_file(off_page).unwrap();
        assert!(census(root, &store).is_empty());
        assert!(store.entries.lock().unwrap().views.is_empty());
    }
    #[test]
    fn compact_pruning_waits_for_successful_scan_and_covers_gc() {
        let root =
            crate::tests::support::TempGuard(crate::tests::support::temp_run_dir("list-cache"));
        let root = root.0.as_path();
        let (gone, writer) = journal_at(root.join("gone"));
        writer
            .event("run_finished", serde_json::json!({"status":"success"}))
            .unwrap();
        drop(writer);
        let store = ReadStore::new(1, policy::DEFAULT_READ_CACHE_BYTES);
        census(root, &store);
        assert_eq!(
            crate::gc_run_directories(root, 0, 0, true, 100).unwrap(),
            vec![gone.clone()]
        );
        journal_at(root.join("remaining"));
        assert!(crate::summaries::list_items_cached(root, 0, &store).is_err());
        assert!(
            store
                .entries
                .lock()
                .unwrap()
                .views
                .contains_key(&run_meta_dir(&gone).join("journal.jsonl"))
        );
        assert_eq!(census(root, &store).len(), 1);
        assert!(
            !store
                .entries
                .lock()
                .unwrap()
                .views
                .contains_key(&run_meta_dir(&gone).join("journal.jsonl"))
        );
        std::fs::remove_dir_all(root).unwrap();
        assert!(census(root, &store).is_empty());
        assert!(store.entries.lock().unwrap().views.is_empty());
    }
    #[test]
    fn compact_and_full_views_share_byte_eviction_and_accounting() {
        let root =
            crate::tests::support::TempGuard(crate::tests::support::temp_run_dir("list-cache"));
        let root = root.0.as_path();
        let (a, _) = journal_at(root.join("a"));
        let (b, _) = journal_at(root.join("b"));
        // Timestamp precision changes String charges even for equal-length paths.
        for (run, ts) in [
            (&a, "2026-01-01T00:00:00.123+00:00"),
            (&b, "2026-01-01T00:00:00.123456789+00:00"),
        ] {
            let path = run_meta_dir(run).join("journal.jsonl");
            let mut event: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            event["ts"] = serde_json::json!(ts);
            std::fs::write(path, format!("{event}\n")).unwrap();
        }
        let probe = ReadStore::new(1, policy::DEFAULT_READ_CACHE_BYTES);
        probe.list_item(&a).unwrap();
        let compact_a = probe.entries.lock().unwrap().bytes;
        probe.read(&a).unwrap();
        let full_a = probe.entries.lock().unwrap().bytes;
        probe.list_item(&b).unwrap();
        let compact_b =
            probe.entries.lock().unwrap().views[&run_meta_dir(&b).join("journal.jsonl")].bytes;
        probe.read(&b).unwrap();
        let full_b = probe.entries.lock().unwrap().bytes;
        let full = full_a.max(full_b);
        assert!(compact_a.max(compact_b) < full_a.min(full_b) / 4);
        // Admit either entry individually, but never both together. Otherwise an
        // oversized b is returned uncached and the final a is still a cache hit.
        let capacity = compact_a.max(compact_b);
        assert!(capacity < compact_a + compact_b);
        assert!(full < full_a + compact_b);
        let store = ReadStore::new(1, capacity);
        for (run, bytes) in [(&a, compact_a), (&b, compact_b), (&a, compact_a)] {
            store.list_item(run).unwrap();
            let cache = store.entries.lock().unwrap();
            let key = run_meta_dir(run).join("journal.jsonl");
            assert_eq!(cache.views.len(), 1);
            assert_eq!(cache.bytes, bytes);
            assert_eq!(cache.views[&key].bytes, bytes);
            assert_eq!(cache.recency.len(), 1);
            assert_eq!(cache.recency.first_key_value().unwrap().0.1, key);
        }
        assert_eq!(store.applied.load(Ordering::Relaxed), 3);
        let store = ReadStore::new(1, full);
        store.read(&a).unwrap();
        store.list_item(&b).unwrap();
        assert!(
            !store
                .entries
                .lock()
                .unwrap()
                .views
                .contains_key(&run_meta_dir(&a).join("journal.jsonl"))
        );
        store.read(&a).unwrap();
        let cache = store.entries.lock().unwrap();
        assert_eq!(cache.full_count, 1);
        assert_eq!(
            cache.bytes,
            cache.views.values().map(|entry| entry.bytes).sum::<usize>()
        );
        assert!(cache.bytes <= full);
        assert_eq!(cache.views.len(), cache.recency.len());
        drop(cache);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..8 {
                        store.list_item(&a).unwrap();
                        store.read(&b).unwrap();
                    }
                });
            }
        });
        let cache = store.entries.lock().unwrap();
        assert!(cache.bytes <= full);
        assert_eq!(cache.full_count, 1);
        assert_eq!(
            cache.bytes,
            cache.views.values().map(|entry| entry.bytes).sum::<usize>()
        );
        assert_eq!(cache.views.len(), cache.recency.len());
        let disabled = ReadStore::new(1, compact_a - 1);
        disabled.list_item(&a).unwrap();
        assert!(disabled.entries.lock().unwrap().views.is_empty());
    }
    #[test]
    fn full_read_compacts_on_list_hit_and_stays_warm() {
        let root =
            crate::tests::support::TempGuard(crate::tests::support::temp_run_dir("list-cache"));
        let root = root.0.as_path();
        let (run, _writer) = journal_at(root.join("a"));
        let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
        store.read(&run).unwrap();
        let full = store.entries.lock().unwrap().bytes;
        assert_eq!(store.entries.lock().unwrap().full_count, 1);
        let applied = store.applied.load(Ordering::Relaxed);
        let read = store.bytes_read.load(Ordering::Relaxed);
        let item = store.list_item(&run).unwrap();
        assert_eq!(item.seq, 1);
        {
            let cache = store.entries.lock().unwrap();
            assert_eq!(cache.full_count, 0);
            assert!(cache.bytes < full / 4);
            let entry = cache
                .views
                .get(&run_meta_dir(&run).join("journal.jsonl"))
                .unwrap();
            assert!(matches!(entry.view, CachedView::List(_)));
            assert!(entry.tail.is_empty());
            assert_eq!(cache.bytes, entry.bytes);
            assert_eq!(cache.views.len(), cache.recency.len());
        }
        assert_eq!(store.applied.load(Ordering::Relaxed), applied);
        assert_eq!(store.bytes_read.load(Ordering::Relaxed), read);
        assert_eq!(census(root, &store).len(), 1);
        assert_eq!(store.applied.load(Ordering::Relaxed), applied);
        assert_eq!(store.bytes_read.load(Ordering::Relaxed), read);
        store.read(&run).unwrap();
        assert_eq!(store.entries.lock().unwrap().full_count, 1);
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
    #[test]
    fn read_includes_complete_final_value_without_trailing_lf() {
        let (directory, writer) = journal();
        writer
            .event("run_started", serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1}))
            .unwrap();
        drop(writer);
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            bytes.ends_with(b"\n"),
            "journal helper must LF-terminate events"
        );
        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
        let scan = engine::read_journal_values(&path, engine::JournalLimits::default()).unwrap();
        assert_eq!(
            scan.events.len(),
            2,
            "authoritative reader keeps final value without LF"
        );
        let authoritative = engine::RunState::fold_journal(&path).unwrap();
        assert_eq!(authoritative.last_seq, 2);
        let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
        let view = store.read(&directory).unwrap();
        assert_eq!(
            view.state.last_seq, authoritative.last_seq,
            "ReadStore must include a complete final JSON value without trailing LF"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn read_ignores_incomplete_tail_then_includes_completed_value() {
        let (directory, writer) = journal();
        writer
            .event("run_started", serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1}))
            .unwrap();
        drop(writer);
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let full = std::fs::read(&path).unwrap();
        let first_end = full.iter().position(|byte| *byte == b'\n').unwrap() + 1;
        let (first_line, second_line) = full.split_at(first_end);
        assert!(
            second_line.ends_with(b"\n"),
            "journal helper must LF-terminate events"
        );
        let body = &second_line[..second_line.len() - 1];
        let cut = body.len() / 2;
        assert!(cut > 0 && cut < body.len());
        let (prefix, remainder) = body.split_at(cut);
        let mut fragment = Vec::with_capacity(first_line.len() + prefix.len());
        fragment.extend_from_slice(first_line);
        fragment.extend_from_slice(prefix);
        std::fs::write(&path, &fragment).unwrap();
        let scan = engine::read_journal_values(&path, engine::JournalLimits::default()).unwrap();
        assert_eq!(
            scan.events.len(),
            1,
            "authoritative reader ignores an incomplete tail"
        );
        let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
        assert_eq!(
            store.read(&directory).unwrap().state.last_seq,
            1,
            "both readers ignore an incomplete final JSON fragment"
        );
        // Complete the fragment without adding a trailing LF: the result is a
        // complete final JSON value at EOF, which the authoritative reader
        // includes.
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(remainder).unwrap();
        }
        let scan = engine::read_journal_values(&path, engine::JournalLimits::default()).unwrap();
        assert_eq!(
            scan.events.len(),
            2,
            "authoritative reader keeps the completed final value"
        );
        let authoritative = engine::RunState::fold_journal(&path).unwrap();
        assert_eq!(authoritative.last_seq, 2);
        let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
        let view = store.read(&directory).unwrap();
        assert_eq!(
            view.state.last_seq, authoritative.last_seq,
            "ReadStore must include the completed final value without trailing LF"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn unterminated_lines_are_not_cached_and_appends_are_reparsed() {
        use std::io::Write;
        let (directory, writer) = journal();
        drop(writer);
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let original = std::fs::read(&path).unwrap();
        let mut whitespace_tail = original.clone();
        whitespace_tail.extend_from_slice(b" \t\r\x0c");
        let mut incomplete_tail = original.clone();
        incomplete_tail.extend_from_slice(b"{");
        for bytes in [
            original[..original.len() - 1].to_vec(),
            whitespace_tail,
            incomplete_tail,
        ] {
            std::fs::write(&path, &bytes).unwrap();
            let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
            let view = store.read(&directory).unwrap();
            assert_eq!(view.state.last_seq, 1);
            let expected_offset = if bytes.ends_with(b"{") {
                original.len()
            } else {
                bytes.len()
            };
            assert_eq!(view.offset, expected_offset as u64);
            assert_eq!(store.list_item(&directory).unwrap().seq, 1);
            assert!(store.entries.lock().unwrap().views.is_empty());
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"x\n").unwrap();
            assert!(engine::read_journal_values(&path, engine::JournalLimits::default()).is_err());
            assert!(store.read(&directory).is_err());
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn malformed_tails_and_lf_terminated_incomplete_json_are_errors() {
        let (directory, writer) = journal();
        drop(writer);
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let original = std::fs::read(&path).unwrap();
        for tail in [b"{\"bad\":!}".as_slice(), b"{\n", b"\x0b", b"\xc2\xa0"] {
            let mut bytes = original.clone();
            bytes.extend_from_slice(tail);
            std::fs::write(&path, bytes).unwrap();
            assert!(engine::read_journal_values(&path, engine::JournalLimits::default()).is_err());
            let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
            assert!(store.read(&directory).is_err());
            assert!(store.entries.lock().unwrap().views.is_empty());
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn event_byte_limit_excludes_only_lf() {
        let (directory, writer) = journal();
        drop(writer);
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let original = std::fs::read(&path).unwrap();
        let limit = engine::JournalLimits::default().max_event_bytes.unwrap();
        // Check both JSON and whitespace: size limits apply before blank-line
        // skipping, and an unterminated body gets no extra byte allowance.
        for whitespace in [false, true] {
            for extra in 0..=2 {
                for has_newline in [false, true] {
                    let mut bytes = if whitespace {
                        Vec::new()
                    } else {
                        original[..original.len() - 1].to_vec()
                    };
                    bytes.resize(limit + extra, b' ');
                    if has_newline {
                        bytes.push(b'\n');
                    }
                    std::fs::write(&path, &bytes).unwrap();
                    let scan = engine::read_journal_values(&path, engine::JournalLimits::default());
                    let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
                    let view = store.read(&directory);
                    assert_eq!(scan.is_ok(), extra == 0);
                    assert_eq!(view.is_ok(), scan.is_ok());
                    if let Ok(view) = view {
                        assert_eq!(view.count, usize::from(!whitespace));
                        assert_eq!(view.offset, bytes.len() as u64);
                        assert_eq!(
                            store.entries.lock().unwrap().views.len(),
                            usize::from(has_newline)
                        );
                    }
                }
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn read_skips_blank_and_whitespace_only_lines() {
        let (directory, writer) = journal();
        writer
            .event("run_started", serde_json::json!({"generator":"test","generator_path":"test","contract_sha256":"abc","inputs":{},"resource_hashes":[],"schema_version":1}))
            .unwrap();
        drop(writer);
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let original = std::fs::read(&path).unwrap();
        let first_end = original.iter().position(|byte| *byte == b'\n').unwrap() + 1;
        let (first_line, rest) = original.split_at(first_end);
        let mut with_blanks = Vec::new();
        with_blanks.extend_from_slice(first_line);
        with_blanks.extend_from_slice(b"\n");
        with_blanks.extend_from_slice(b"   \n");
        with_blanks.extend_from_slice(rest);
        with_blanks.extend_from_slice(b"\t \t\n");
        std::fs::write(&path, &with_blanks).unwrap();
        let scan = engine::read_journal_values(&path, engine::JournalLimits::default()).unwrap();
        assert_eq!(
            scan.events.len(),
            2,
            "authoritative reader skips whitespace-only lines"
        );
        let authoritative = engine::RunState::fold_journal(&path).unwrap();
        assert_eq!(authoritative.last_seq, 2);
        let store = ReadStore::new(8, policy::DEFAULT_READ_CACHE_BYTES);
        let view = store
            .read(&directory)
            .expect("ReadStore must accept blank and ASCII-whitespace-only lines");
        assert_eq!(
            view.state.last_seq, authoritative.last_seq,
            "ReadStore must skip blank and whitespace-only lines like the engine reader"
        );
        assert_eq!(view.offset, with_blanks.len() as u64);
        assert_eq!(store.entries.lock().unwrap().views.len(), 1);
        let read = store.bytes_read.load(Ordering::Relaxed);
        store.read(&directory).unwrap();
        assert_eq!(store.bytes_read.load(Ordering::Relaxed), read);
        std::fs::remove_dir_all(directory).unwrap();
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
