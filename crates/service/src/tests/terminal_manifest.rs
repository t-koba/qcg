//! Deterministic publication interleavings for run snapshots.
use camino::{Utf8Path, Utf8PathBuf};
use std::sync::Mutex;

static AFTER_FIRST_READ: Mutex<Option<Utf8PathBuf>> = Mutex::new(None);

pub(crate) fn arm(directory: &Utf8Path) {
    assert!(
        AFTER_FIRST_READ
            .lock()
            .unwrap()
            .replace(directory.to_owned())
            .is_none()
    );
}

pub(crate) fn after_first_read(directory: &Utf8Path) {
    let armed = {
        let mut hook = AFTER_FIRST_READ.lock().unwrap();
        if hook.as_deref() == Some(directory) {
            hook.take().is_some()
        } else {
            false
        }
    };
    if !armed {
        return;
    }
    let meta = directory.join("meta");
    std::fs::write(meta.join("outputs.json"), br#"{"artifacts":[]}"#)
        .expect("manifest publication should succeed");
    let id = directory.file_name().expect("run id should exist");
    engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None)
        .expect("journal should reopen")
        .event("run_finished", serde_json::json!({"status":"success"}))
        .expect("terminal publication should succeed");
}
