use super::support::*;
use crate::*;
use api::{RunListItem, RunStatus};
use camino::Utf8Path;
use serde_json::json;
use std::sync::atomic::Ordering;

/// Writes one run whose identity event carries the given `started_at` and
/// generator; `status` is `None` for a run that is still active.
fn write_run(
    runs: &Utf8Path,
    id: &str,
    started_at: &str,
    generator: &str,
    status: Option<&str>,
) -> engine::JournalWriter {
    let meta = runs.join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let path = meta.join("journal.jsonl");
    let journal = engine::JournalWriter::create(&path, id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": format!("{generator}@1.0.0"), "generator_path": "demo",
                "contract_sha256": "test", "inputs": {}, "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            }),
        )
        .unwrap();
    if let Some(status) = status {
        journal
            .event("run_finished", json!({"status": status}))
            .unwrap();
    }
    // The writer stamps the current time; rewrite the identity event's ts.
    // A live run keeps its stamp so its open writer's checkpoint stays valid.
    if status.is_none() {
        return journal;
    }
    let text = std::fs::read_to_string(&path).unwrap();
    let (first, rest) = text.split_once('\n').unwrap();
    let mut event: serde_json::Value = serde_json::from_str(first).unwrap();
    event["ts"] = json!(started_at);
    std::fs::write(&path, format!("{event}\n{rest}")).unwrap();
    journal
}

fn service(runs: &Utf8Path) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        vec![],
        runs.to_owned(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .unwrap()
}

fn page(limit: usize) -> RunPage {
    RunPage {
        limit,
        ..Default::default()
    }
}

async fn ids(service: &LocalService, page: RunPage) -> Vec<String> {
    let response = service.list_run_page(page).await.unwrap();
    response.items.into_iter().map(|item| item.run_id).collect()
}

fn corrupt(runs: &Utf8Path, id: &str) {
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(runs.join(id).join("meta/journal.jsonl"))
        .unwrap()
        .write_all(b"broken\n")
        .unwrap();
}

#[tokio::test]
async fn a_page_validates_the_runs_it_examines_not_unrelated_history() {
    let root = temp_run_dir("run-page-validation");
    let _guard = TempGuard(root.clone());
    for (index, id) in ["r1", "r2", "r3", "r4"].into_iter().enumerate() {
        write_run(
            &root,
            id,
            &format!("2026-01-0{}T00:00:00Z", index + 1),
            "demo",
            Some("success"),
        );
    }
    let service = service(&root);
    assert_eq!(ids(&service, page(4)).await.len(), 4);
    corrupt(&root, "r4");
    // The page examines limit + 1 runs, so r4 is off the first page only
    // while it is not the run that decides `next_cursor`.
    assert_eq!(ids(&service, page(2)).await, ["r1", "r2"]);
    assert_eq!(
        ids(
            &service,
            RunPage {
                cursor: Some(("2026-01-01T00:00:00Z".into(), "r1".into())),
                ..page(1)
            }
        )
        .await,
        ["r2"]
    );
    // The same run fails every page that includes it, and detail reads.
    assert!(service.list_run_page(page(3)).await.is_err());
    assert!(service.list_run_items().await.is_err());
    let newest_first = RunPage {
        descending: true,
        ..page(1)
    };
    assert!(service.list_run_page(newest_first).await.is_err());
    assert!(service.snapshot("r4".into()).await.is_err());
}

#[tokio::test]
async fn warm_page_requests_do_no_journal_reads_proportional_to_history() {
    let root = temp_run_dir("run-page-bounded");
    let _guard = TempGuard(root.clone());
    for index in 0..60 {
        write_run(
            &root,
            &format!("run-{index:03}"),
            &format!("2026-01-01T00:00:{index:02}Z"),
            "demo",
            Some("success"),
        );
    }
    let service = service(&root);
    let store = service.inner.read_store.clone();
    // The first request after start learns every run's ordering facts once.
    assert_eq!(ids(&service, page(1)).await, ["run-000"]);
    let learned = store.applied.load(Ordering::Relaxed);
    assert!(learned >= 60);
    let (applied, bytes) = (
        store.applied.load(Ordering::Relaxed),
        store.bytes_read.load(Ordering::Relaxed),
    );
    for descending in [false, true] {
        assert_eq!(
            ids(
                &service,
                RunPage {
                    descending,
                    ..page(1)
                }
            )
            .await
            .len(),
            1
        );
    }
    assert_eq!(store.applied.load(Ordering::Relaxed), applied);
    assert_eq!(store.bytes_read.load(Ordering::Relaxed), bytes);
    // A restarted service re-learns the facts lazily from the same store.
    let restarted = self::service(&root);
    ids(&restarted, page(1)).await;
    assert_eq!(
        restarted.inner.read_store.applied.load(Ordering::Relaxed),
        learned
    );
}

#[tokio::test]
async fn changes_by_another_process_are_visible_on_the_next_request() {
    let root = temp_run_dir("run-page-shared");
    let _guard = TempGuard(root.clone());
    let service = service(&root);
    let active = write_run(&root, "a", "2026-01-01T00:00:00Z", "demo", None);
    write_run(&root, "b", "2099-01-02T00:00:00Z", "demo", Some("success"));
    let states = |items: Vec<RunListItem>| {
        items
            .into_iter()
            .map(|item| (item.run_id, item.state))
            .collect::<Vec<_>>()
    };
    let list = || async { states(service.list_run_page(page(10)).await.unwrap().items) };
    assert_eq!(
        list().await,
        [
            ("a".into(), RunStatus::Running),
            ("b".into(), RunStatus::Succeeded)
        ]
    );
    // Update: an append to an already-listed run.
    active
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    assert_eq!(list().await[0].1, RunStatus::Succeeded);
    // Add: a new directory.
    write_run(&root, "c", "2099-01-03T00:00:00Z", "other", Some("failed"));
    assert_eq!(ids(&service, page(10)).await, ["a", "b", "c"]);
    let other = RunPage {
        generator_id: Some("other".into()),
        ..page(10)
    };
    assert_eq!(ids(&service, other).await, ["c"]);
    // Delete and garbage-collect.
    std::fs::remove_dir_all(root.join("b")).unwrap();
    assert_eq!(ids(&service, page(10)).await, ["a", "c"]);
    assert_eq!(gc_run_directories(&root, 0, 0, true, 100).unwrap().len(), 2);
    assert!(ids(&service, page(10)).await.is_empty());
    // A directory that stops being a run (journal emptied) is excluded.
    write_run(&root, "d", "2099-01-04T00:00:00Z", "demo", Some("success"));
    assert_eq!(ids(&service, page(10)).await, ["d"]);
    std::fs::write(root.join("d/meta/journal.jsonl"), []).unwrap();
    assert!(ids(&service, page(10)).await.is_empty());
}

#[tokio::test]
async fn a_replaced_journal_reorders_the_page_it_is_listed_on() {
    let root = temp_run_dir("run-page-replaced");
    let _guard = TempGuard(root.clone());
    write_run(&root, "a", "2026-01-01T00:00:00Z", "demo", Some("success"));
    write_run(&root, "b", "2026-01-02T00:00:00Z", "demo", Some("success"));
    let service = service(&root);
    assert_eq!(ids(&service, page(10)).await, ["a", "b"]);
    std::fs::remove_dir_all(root.join("a")).unwrap();
    write_run(&root, "a", "2026-01-03T00:00:00Z", "other", Some("success"));
    assert_eq!(ids(&service, page(10)).await, ["b", "a"]);
    let other = RunPage {
        generator_id: Some("other".into()),
        ..page(10)
    };
    assert_eq!(ids(&service, other).await, ["a"]);
}

/// The previous implementation: filter, order and cut a full census.
fn reference(mut items: Vec<RunListItem>, page: &RunPage) -> api::RunListResponse {
    items.retain(|item| {
        page.state.is_none_or(|state| item.state == state)
            && page
                .generator_id
                .as_ref()
                .is_none_or(|id| &item.generator_id == id)
            && page.since.is_none_or(|since| {
                chrono::DateTime::parse_from_rfc3339(&item.started_at).is_ok_and(|at| at >= since)
            })
            && page.cursor.as_ref().is_none_or(|(at, id)| {
                if page.descending {
                    (&item.started_at, &item.run_id) < (at, id)
                } else {
                    (&item.started_at, &item.run_id) > (at, id)
                }
            })
    });
    if page.descending {
        items.reverse();
    }
    let next_cursor = (items.len() > page.limit).then(|| {
        let last = &items[page.limit - 1];
        format!("{}|{}", last.started_at, last.run_id)
    });
    items.truncate(page.limit);
    api::RunListResponse { items, next_cursor }
}

#[tokio::test]
async fn pages_match_the_full_census_reference_on_a_randomized_store() {
    let root = temp_run_dir("run-page-reference");
    let _guard = TempGuard(root.clone());
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    let zones = ["+00:00", "+09:00", "-05:00", "Z"];
    let service = service(&root);
    let mut writers = Vec::new();
    for index in 0..48 {
        // Few distinct instants and mixed offsets force ties and
        // textual order that differs from chronological order.
        let started_at = format!(
            "2026-01-0{}T{:02}:00:00{}",
            1 + next(3),
            next(4) * 6,
            zones[next(4) as usize]
        );
        let generator = ["alpha", "beta", "gamma"][next(3) as usize];
        let status = [None, Some("success"), Some("failed")][next(3) as usize];
        writers.push(write_run(
            &root,
            &format!("run-{index:02}"),
            &started_at,
            generator,
            status,
        ));
    }
    let census = service.list_run_items().await.unwrap();
    assert_eq!(census.len(), 48);
    let since = chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00+00:00").unwrap();
    for descending in [false, true] {
        for generator_id in [None, Some("beta".to_string())] {
            for state in [None, Some(RunStatus::Succeeded), Some(RunStatus::Running)] {
                for since in [None, Some(since)] {
                    for limit in [1, 5, 100] {
                        let mut query = RunPage {
                            limit,
                            since,
                            cursor: None,
                            state,
                            generator_id: generator_id.clone(),
                            descending,
                        };
                        let (mut got, mut want) = (Vec::new(), Vec::new());
                        loop {
                            let page = service
                                .list_run_page(RunPage {
                                    cursor: query.cursor.clone(),
                                    generator_id: generator_id.clone(),
                                    ..query
                                })
                                .await
                                .unwrap();
                            let expected = reference(census.clone(), &query);
                            assert_eq!(
                                serde_json::to_vec(&page).unwrap(),
                                serde_json::to_vec(&expected).unwrap()
                            );
                            got.extend(page.items);
                            want.extend(expected.items);
                            let Some(cursor) = expected.next_cursor else {
                                break;
                            };
                            let (at, id) = cursor.split_once('|').unwrap();
                            query.cursor = Some((at.into(), id.into()));
                        }
                        assert_eq!(got.len(), want.len());
                    }
                }
            }
        }
    }
    drop(writers);
}
