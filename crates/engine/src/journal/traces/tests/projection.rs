use super::*;

fn source() -> (Fixture, Vec<Value>) {
    let f = Fixture::new();
    let writer = f.writer().with_read_recorder(reads("node")).unwrap();
    finish(&writer, "node", "success");
    finish(
        &writer.with_read_recorder(reads("later")).unwrap(),
        "later",
        "success",
    );
    let events = read_journal_values(&f.path, JournalLimits::default())
        .unwrap()
        .events;
    (f, events)
}

fn child(event: &Value, seq: u64) -> Value {
    let mut child = event.clone();
    child["seq"] = json!(seq);
    child["run_id"] = json!("child");
    child["trace_id"] = json!(api::trace_id_for_run("child"));
    child["span_id"] = json!(api::span_id_for_seq(seq));
    if child.get("parent_span_id").is_some() {
        child["parent_span_id"] = json!(api::span_id_for_scope("child", "step:node"));
    }
    child
}

fn write_events(f: &Fixture, events: &[Value]) {
    let mut file = std::fs::File::create(&f.path).unwrap();
    for event in events {
        serde_json::to_writer(&mut file, event).unwrap();
        file.write_all(b"\n").unwrap();
    }
    file.sync_all().unwrap();
}

#[test]
fn fork_projection_rebinds_both_digests_and_restores_only_exact_latest_terminal() {
    let (source, events) = source();
    let target = Fixture::new();
    let child = child(&events[0], 5);
    write_events(&target, std::slice::from_ref(&child));
    let mut projection =
        ForkTraceProjection::new(&source.path, &events[..1], 1, JournalLimits::default()).unwrap();
    projection.bind(1, &child).unwrap();
    projection.publish(&target.path);
    let records = target.records();
    assert_eq!(
        records.len(),
        1,
        "records past the cutoff are not projected"
    );
    assert_ne!(
        records[0]["record_digest"],
        source.records()[0]["record_digest"]
    );
    assert_ne!(
        records[0]["event_digest"],
        source.records()[0]["event_digest"]
    );
    assert_eq!(records[0]["seq"], 5);
    assert_eq!(records[0]["event_digest"], json!(digest(&child).unwrap()));
    let record: Record = serde_json::from_value(records[0].clone()).unwrap();
    assert_eq!(record.calculate_digest(), Some(record.record_digest));
    assert_trace(&target.fold(), "node", 5);
    assert_trace(&target.writer().state(), "node", 5);
    assert!(!target.fold().dependency_traces.owners.contains_key("later"));

    let mut altered = child.clone();
    altered["status"] = json!("repaired");
    write_events(&target, &[altered]);
    assert_success(&target.fold(), "node");
    assert!(target.fold().dependency_traces.owners.is_empty());
    write_events(&target, &[child]);
    target
        .writer()
        .event(
            "step_started",
            json!({"node":"node", "type":"test", "attempt":1}),
        )
        .unwrap();
    assert!(target.fold().dependency_traces.owners.is_empty());
}

#[test]
fn fork_projection_rejects_corrupt_duplicate_incomplete_and_unsupported_records() {
    let (source, events) = source();
    let original = source.records();
    for mode in [
        "checksum",
        "duplicate",
        "version",
        "owner",
        "incomplete",
        "unsupported",
        "event_digest",
        "seq",
        "partial",
        "json",
    ] {
        let mut records = original.clone();
        match mode {
            "checksum" => records[0]["record_digest"] = json!(vec![0u8; 32]),
            "duplicate" => records.push(records[0].clone()),
            "partial" | "json" => {}
            _ => {
                let mut record: Record = serde_json::from_value(records[0].clone()).unwrap();
                match mode {
                    "version" => record.version = 1,
                    "owner" => record.owner = "other".into(),
                    "incomplete" => record.complete = false,
                    "unsupported" => record.reads[0].identity = Identity::Unsupported,
                    "event_digest" => record.event_digest = [0; 32],
                    "seq" => record.seq = 0,
                    _ => unreachable!(),
                }
                record.record_digest = record.calculate_digest().unwrap();
                records[0] = serde_json::to_value(record).unwrap();
            }
        }
        source.replace(&records);
        let path = source.path.with_file_name(FILE_NAME);
        if mode == "partial" {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        } else if mode == "json" {
            std::fs::write(&path, b"not-json\n").unwrap();
        }
        assert!(
            ForkTraceProjection::new(&source.path, &events, 2, JournalLimits::default()).is_none(),
            "{mode}"
        );
        assert_success(&source.fold(), "node");
    }
}

#[test]
fn fork_projection_requires_one_selected_source_and_one_corresponding_child() {
    let (source, events) = source();
    let new = |selected: &[Value]| {
        ForkTraceProjection::new(&source.path, selected, 1, JournalLimits::default())
    };
    assert!(
        new(&events[1..]).is_none(),
        "unselected/audit-only identity cannot match"
    );
    assert!(
        new(&[events[0].clone(), events[0].clone()]).is_none(),
        "even identical source ties are ambiguous"
    );
    let mut mismatched = events[..1].to_vec();
    mismatched[0]["status"] = json!("repaired");
    assert!(new(&mismatched).is_none());

    let target = Fixture::new();
    new(&events[..1]).unwrap().publish(&target.path);
    assert!(
        !target.path.with_file_name(FILE_NAME).exists(),
        "unbound output is never published"
    );
    let mut wrong_child = child(&events[0], 4);
    wrong_child["status"] = json!("repaired");
    assert!(new(&events[..1]).unwrap().bind(1, &wrong_child).is_none());
    let mut projection = new(&events[..1]).unwrap();
    projection.bind(1, &child(&events[0], 4)).unwrap();
    assert!(projection.bind(1, &child(&events[0], 5)).is_none());
}

#[test]
fn fork_projection_output_bound_failure_removes_temp_and_preserves_replay() {
    let (source, events) = source();
    let target = Fixture::new();
    let source_bytes = std::fs::metadata(source.path.with_file_name(FILE_NAME))
        .unwrap()
        .len() as usize;
    // Digest arrays have variable JSON byte widths. Choose a deterministic
    // child identity that actually exceeds the source stream's exact bound.
    let children = (0..128)
        .find_map(|nonce| {
            let children = [
                child(&events[0], 10_000_000_000_000_000_000),
                child(&events[1], 10_000_000_000_000_000_001),
            ]
            .map(|mut event| {
                event["run_id"] = json!(format!("child-{nonce}"));
                event
            });
            let bytes: usize = source
                .records()
                .into_iter()
                .zip(&children)
                .map(|(record, event)| {
                    let mut record: Record = serde_json::from_value(record).unwrap();
                    record.seq = event["seq"].as_u64().unwrap();
                    record.event_digest = digest(event).unwrap();
                    record.record_digest = record.calculate_digest().unwrap();
                    serde_json::to_vec(&record).unwrap().len() + 1
                })
                .sum();
            (bytes > source_bytes).then_some(children)
        })
        .expect("child records must exceed the selected source byte bound");
    write_events(&target, &children);
    let limits = JournalLimits {
        max_total_bytes: Some(source_bytes),
        ..JournalLimits::default()
    };
    let mut projection = ForkTraceProjection::new(&source.path, &events, 2, limits).unwrap();
    for (seq, child) in children.iter().enumerate() {
        projection.bind(seq as u64 + 1, child).unwrap();
    }
    projection.publish(&target.path);
    assert!(!target.path.with_file_name(FILE_NAME).exists());
    assert!(
        std::fs::read_dir(target.path.parent().unwrap())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".part-"))
    );
    assert_success(&target.fold(), "node");
    assert_success(&target.fold(), "later");
    assert!(target.fold().dependency_traces.owners.is_empty());
}
