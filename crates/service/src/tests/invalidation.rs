use super::support::*;
use crate::*;
use api::{ApiError, InvalidateRun, RunStatus};
use camino::Utf8Path;
use serde_json::json;

fn request() -> InvalidateRun {
    InvalidateRun {
        reason: "wrong output".into(),
        actor_label: "reviewer".into(),
    }
}

fn write_run(runs: &Utf8Path, id: &str, status: &str) {
    let meta = runs.join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
                "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            }),
        )
        .unwrap();
    match status {
        "active" => {}
        "canceled" | "interrupted" => {
            journal
                .event(
                    &format!("run_{status}"),
                    json!({"reason": {"code": status, "message": "test"}}),
                )
                .unwrap();
        }
        _ => {
            journal
                .event("run_finished", json!({"status": status}))
                .unwrap();
        }
    }
}

fn shared(runs: &Utf8Path) -> LocalService {
    shared_with_policy(runs, ServiceDeploymentPolicy::default())
}

fn shared_with_policy(runs: &Utf8Path, deployment: ServiceDeploymentPolicy) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        vec![],
        runs.to_owned(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        deployment,
    )
    .unwrap()
}

#[tokio::test]
async fn invalidation_is_success_only_immutable_and_visible_to_cached_peers() {
    let root = temp_run_dir("invalidation");
    let _guard = TempGuard(root.clone());
    for status in ["success", "failed", "canceled", "interrupted"] {
        write_run(&root, status, status);
    }
    let owner = shared(&root);
    let peer = shared(&root);
    write_run(&root, "active", "active");
    let before = peer.snapshot("success".into()).await.unwrap();
    assert!(before.invalidation.is_none());
    assert!(
        peer.list_run_items()
            .await
            .unwrap()
            .iter()
            .all(|r| r.invalidation.is_none())
    );
    let journal = std::fs::read(root.join("success/meta/journal.jsonl")).unwrap();
    let record = owner.invalidate_run("success", request()).await.unwrap();
    assert_eq!(
        owner.invalidate_run("success", request()).await.unwrap(),
        record
    );
    let mut different = request();
    different.actor_label = "other".into();
    assert!(matches!(
        owner.invalidate_run("success", different).await,
        Err(ApiError::Conflict { .. })
    ));
    let mut different = request();
    different.reason = "different".into();
    assert!(matches!(
        owner.invalidate_run("success", different).await,
        Err(ApiError::Conflict { .. })
    ));
    for id in ["failed", "canceled", "interrupted", "active"] {
        assert!(
            matches!(
                owner.invalidate_run(id, request()).await,
                Err(ApiError::Conflict { .. })
            ),
            "{id}"
        );
        assert!(!root.join(id).join("meta/invalidation.json").exists());
    }
    assert!(matches!(
        owner.invalidate_run("missing", request()).await,
        Err(ApiError::NotFound { .. })
    ));
    assert!(!root.join("missing").exists());
    for (reason, actor) in [
        (" ".into(), "ok".into()),
        ("bad\nreason".into(), "ok".into()),
        ("é".repeat(2049), "ok".into()),
        ("ok".into(), "é".repeat(65)),
        ("ok".into(), "\0".into()),
    ] {
        assert!(matches!(
            owner
                .invalidate_run(
                    "success",
                    InvalidateRun {
                        reason,
                        actor_label: actor
                    }
                )
                .await,
            Err(ApiError::Invalid { .. })
        ));
    }
    let after = peer.snapshot("success".into()).await.unwrap();
    assert_eq!(after.state, RunStatus::Succeeded);
    assert_eq!(after.seq, before.seq);
    assert_eq!(after.invalidation.as_ref(), Some(&record));
    assert_eq!(
        peer.list_run_items()
            .await
            .unwrap()
            .iter()
            .find(|r| r.run_id == "success")
            .unwrap()
            .invalidation
            .as_ref(),
        Some(&record)
    );
    let bundle_parts = owner
        .run_bundle_parts("success", &ArtifactZipLimits::default())
        .await
        .unwrap();
    let bundle_snapshot: api::RunSnapshot =
        serde_json::from_slice(&bundle_parts.snapshot_bytes).unwrap();
    assert_eq!(bundle_snapshot.invalidation.as_ref(), Some(&record));
    assert_eq!(
        std::fs::read(root.join("success/meta/journal.jsonl")).unwrap(),
        journal
    );
    // A peer deletion leaves stale memory in owner. Neither mutation may
    // recreate the run through its execution lease.
    peer.delete_run("success").await.unwrap();
    assert!(matches!(
        owner.invalidate_run("success", request()).await,
        Err(ApiError::NotFound { .. })
    ));
    assert!(matches!(
        owner.delete_run("success").await,
        Err(ApiError::NotFound { .. })
    ));
    assert!(!root.join("success").exists());
}

#[tokio::test]
async fn invalidation_delete_and_gc_share_stable_maintenance_coordination() {
    let root = temp_run_dir("invalidation-locks");
    let _guard = TempGuard(root.clone());
    let service = shared_with_policy(
        &root,
        ServiceDeploymentPolicy {
            gc_keep: 0,
            ..Default::default()
        },
    );
    write_run(&root, "success", "success");
    let lock = try_lock_store_maintenance(&root).unwrap().unwrap();
    assert!(matches!(
        service.invalidate_run("success", request()).await,
        Err(ApiError::Conflict { .. })
    ));
    assert!(matches!(
        service.delete_run("success").await,
        Err(ApiError::Conflict { .. })
    ));
    // Auto-GC treats contention as a normal skip.
    service.collect_retained_runs().await.unwrap();
    assert!(root.join("success").exists());
    drop(lock);
    let admission = crate::run_dirs::try_lock_run_admission(&root.join("success"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.delete_run("success").await,
        Err(ApiError::Conflict { .. })
    ));
    assert!(root.join("success").exists());
    drop(admission);
    let handoff = crate::run_dirs::try_lock_run_handoff(&root.join("success"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.delete_run("success").await,
        Err(ApiError::Conflict { .. })
    ));
    assert!(root.join("success").exists());
    drop(handoff);
    service.invalidate_run("success", request()).await.unwrap();
    service.collect_retained_runs().await.unwrap();
    assert!(!root.join("success").exists());
    assert!(matches!(
        service.invalidate_run("success", request()).await,
        Err(ApiError::NotFound { .. })
    ));
    assert!(!root.join("success").exists());
}

// These safety checks reject corrupted, linked, and oversized records across
// platforms. The symlink assertion is Unix-only; Windows has a separate open
// path that checks reparse-point and link metadata on the opened handle.
#[tokio::test]
async fn invalidation_corrupt_linked_mismatched_and_oversized_records_fail_closed() {
    let root = temp_run_dir("invalidation-corrupt");
    let _guard = TempGuard(root.clone());
    let service = shared(&root);
    write_run(&root, "success", "success");
    let record = service.invalidate_run("success", request()).await.unwrap();
    let path = root.join("success/meta/invalidation.json");
    for bytes in [
        b"{".to_vec(),
        vec![b' '; 32769],
        serde_json::to_vec(&api::RunInvalidation {
            run_id: "other".into(),
            ..record.clone()
        })
        .unwrap(),
    ] {
        std::fs::write(&path, bytes).unwrap();
        assert!(service.snapshot("success".into()).await.is_err());
        assert!(service.list_run_items().await.is_err());
    }
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
    let other = root.join("linked.json");
    std::fs::hard_link(&path, &other).unwrap();
    assert!(service.snapshot("success".into()).await.is_err());
    std::fs::remove_file(&other).unwrap();
    #[cfg(unix)]
    {
        std::fs::rename(&path, &other).unwrap();
        std::os::unix::fs::symlink(&other, &path).unwrap();
        assert!(service.snapshot("success".into()).await.is_err());
        assert!(service.list_run_items().await.is_err());
    }
    // Metadata never prevents explicit deletion of a terminal run.
    service.delete_run("success").await.unwrap();
}

#[tokio::test]
async fn invalidation_allows_forks_without_inheriting_metadata() {
    let root = temp_run_dir("invalidation-fork");
    let _guard = TempGuard(root.clone());
    let generators =
        camino::Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = start_ask_user_and_answer(&service, "brief").await;
    wait_for_terminal_snapshot(&service, &id).await;
    let dir = service.run_dir_for(&id).await.unwrap();
    let checkpoint = read_run_events(&dir)
        .unwrap()
        .iter()
        .find(|event| event.kind == "run_waiting")
        .unwrap()
        .seq;
    // Terminal snapshots can precede lease release by a scheduling turn.
    for _ in 0..200 {
        match service.invalidate_run(&id, request()).await {
            Ok(_) => break,
            Err(ApiError::Conflict { .. }) => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await
            }
            Err(error) => panic!("{error}"),
        }
    }
    assert!(
        service
            .snapshot(id.clone())
            .await
            .unwrap()
            .invalidation
            .is_some()
    );
    let fork = service
        .fork_run(
            &id,
            api::ForkRun {
                at_seq: checkpoint,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let snapshot = wait_for_snapshot(&service, &fork, RunStatus::Waiting).await;
    assert!(snapshot.invalidation.is_none());
    assert!(
        !root
            .join("runs")
            .join(&fork)
            .join("meta/invalidation.json")
            .exists()
    );
    service.shutdown_active_runs().await.unwrap();
}

#[tokio::test]
async fn invalidation_and_deletion_recheck_durable_state_not_stale_memory() {
    let root = temp_run_dir("invalidation-stale-memory");
    let _guard = TempGuard(root.clone());
    write_run(&root, "success", "success");
    let service = shared(&root);
    let mut stale = synthetic_hitl_record(&root.join("synthetic"), "success");
    stale.run_dir = root.join("success");
    stale.state = RunStatus::Succeeded;
    service
        .inner
        .runs
        .write()
        .await
        .insert("success".into(), stale);
    // The durable journal, not a cached terminal label, is authority.
    let path = root.join("success/meta/journal.jsonl");
    let journal = std::fs::read_to_string(&path).unwrap();
    let first_line = journal.lines().next().unwrap();
    std::fs::write(&path, format!("{first_line}\n")).unwrap();
    assert!(matches!(
        service.invalidate_run("success", request()).await,
        Err(ApiError::Conflict { .. })
    ));
    assert!(matches!(
        service.delete_run("success").await,
        Err(ApiError::Conflict { .. })
    ));
    assert!(root.join("success").exists());
    assert!(!root.join("success/meta/invalidation.json").exists());
    std::fs::remove_dir_all(root.join("success")).unwrap();
    assert!(matches!(
        service.invalidate_run("success", request()).await,
        Err(ApiError::NotFound { .. })
    ));
    assert!(matches!(
        service.delete_run("success").await,
        Err(ApiError::NotFound { .. })
    ));
    assert!(!root.join("success").exists());
}

#[tokio::test]
async fn invalidation_racing_peer_deletion_never_resurrects_run_directory() {
    let root = temp_run_dir("invalidation-delete-race");
    let _guard = TempGuard(root.clone());
    let owner = shared(&root);
    let peer = shared(&root);
    for i in 0..8 {
        let id = format!("success-{i}");
        write_run(&root, &id, "success");
        let (invalidation, deletion) =
            tokio::join!(owner.invalidate_run(&id, request()), peer.delete_run(&id));
        assert!(
            invalidation.is_ok()
                || matches!(
                    invalidation,
                    Err(ApiError::Conflict { .. } | ApiError::NotFound { .. })
                )
        );
        if let Err(error) = deletion {
            assert!(matches!(error, ApiError::Conflict { .. }));
            peer.delete_run(&id).await.unwrap();
        }
        assert!(!root.join(&id).exists());
        assert!(matches!(
            owner.invalidate_run(&id, request()).await,
            Err(ApiError::NotFound { .. })
        ));
        assert!(!root.join(&id).exists());
    }
}
