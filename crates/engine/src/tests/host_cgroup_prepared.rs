//! Prepared intent is evidence, never authority over an existing cgroup.
use super::*;
use std::os::unix::fs::MetadataExt;
use std::process::{Child, Command};
use std::time::Duration;

fn meta(label: &str) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-prepared-{label}-{}",
        uuid::Uuid::now_v7().as_simple()
    )))
    .unwrap()
}

fn prepared(parent: PathBuf) -> HostCgroupBinding {
    let nonce = uuid::Uuid::now_v7().as_simple().to_string();
    HostCgroupBinding {
        version: HOST_CGROUP_BINDING_VERSION,
        run_id: "run".into(),
        run_root: parent.join(run_root_name("run", &nonce)),
        nonce,
        delegated_parent: parent,
        boot_id: current_boot_id().unwrap(),
        prepared: true,
        cleaned: false,
        empty_confirmed: false,
    }
}

// Test-only fixture placement for a Prepared candidate. This manually mirrors
// the intended path and is never authority: production yields no scope while
// Prepared (`binding.scope()` is `None`), so no launch, kill, or removal can
// be authorized from the intent.
fn candidate_scope(binding: &HostCgroupBinding) -> HostCgroupScope {
    HostCgroupScope {
        delegated_parent: binding.delegated_parent.clone(),
        run_root: binding.run_root.clone(),
        run_id: binding.run_id.clone(),
        nonce: binding.nonce.clone(),
        boot_id: binding.boot_id.clone(),
    }
}

#[test]
fn legacy_v1_is_bound_not_prepared() {
    let meta = meta("legacy");
    let mut binding = prepared("/sys/fs/cgroup/removed-parent".into());
    binding.version = 1;
    binding.prepared = false;
    let mut json = serde_json::to_value(&binding).unwrap();
    json.as_object_mut().unwrap().remove("prepared");
    std::fs::create_dir_all(&meta).unwrap();
    std::fs::write(binding_path(&meta), serde_json::to_vec(&json).unwrap()).unwrap();
    assert_eq!(load_host_binding(&meta).unwrap().unwrap(), binding);
    validate_binding_record(&binding, "run").unwrap();
    assert!(
        binding.scope().is_some(),
        "legacy v1 without prepared is Bound"
    );
    binding.cleaned = true;
    binding.empty_confirmed = true;
    write_binding_atomically(&meta, &binding).unwrap();
    assert_eq!(
        reconcile_host_cgroup(&meta, &HostCgroupConfig::disabled(), "run").unwrap(),
        HostCgroupReconcile::AlreadyCleaned
    );
    // v1 must never smuggle Prepared semantics into an old binding.
    binding.prepared = true;
    assert!(validate_binding_record(&binding, "run").is_err());
    assert!(binding.scope().is_none(), "Prepared never yields a scope");
    binding.version = HOST_CGROUP_BINDING_VERSION;
    binding.cleaned = false;
    assert!(validate_binding_record(&binding, "run").is_err());
    assert!(binding.scope().is_none(), "Prepared never yields a scope");
    std::fs::remove_dir_all(meta).unwrap();
}

// Exercise the retirement mechanism without delegation. Public reconciliation's
// Linux identity/parent checks are exercised separately in delegated tests.
#[test]
fn prepared_retirement_preserves_present_and_uninspectable_candidates() {
    let meta = meta("inspection");
    let parent = meta.join("parent");
    std::fs::create_dir_all(&parent).unwrap();
    let binding = prepared(parent.as_std_path().into());
    write_binding_atomically(&meta, &binding).unwrap();
    let before = std::fs::read(binding_path(&meta)).unwrap();
    std::fs::create_dir(&binding.run_root).unwrap();
    let kill_file = binding.run_root.join("cgroup.kill");
    std::fs::write(&kill_file, b"untouched").unwrap();
    assert!(matches!(
        retire_binding(&meta, binding.clone()),
        Err(HostCgroupError::Identity(_))
    ));
    assert_eq!(std::fs::read(&kill_file).unwrap(), b"untouched");
    std::fs::remove_file(kill_file).unwrap();
    std::fs::remove_dir(&binding.run_root).unwrap();
    std::fs::remove_dir(&parent).unwrap();
    // ENOTDIR is an inspection error, not proof of absence.
    std::fs::write(&parent, b"not a directory").unwrap();
    assert!(matches!(
        retire_binding(&meta, binding.clone()),
        Err(HostCgroupError::Identity(_))
    ));
    assert_eq!(std::fs::read(binding_path(&meta)).unwrap(), before);
    assert_eq!(std::fs::read(&parent).unwrap(), b"not a directory");
    std::fs::remove_file(&parent).unwrap();
    std::fs::create_dir(&parent).unwrap();
    retire_binding(&meta, binding).unwrap();
    let retired = load_host_binding(&meta).unwrap().unwrap();
    assert!(retired.prepared && retired.cleaned && retired.empty_confirmed);
    std::fs::remove_dir_all(meta).unwrap();
}

fn config() -> Option<HostCgroupConfig> {
    if !host_cgroup_tests_enabled() {
        eprintln!("SKIP Prepared crash tests: QCG_HOST_CGROUP_PARENT absent");
        return None;
    }
    Some(host_cgroup_config_from_env().unwrap())
}

#[test]
fn prepared_absent_candidate_retires_only_after_identity_validation() {
    let Some(config) = config() else { return };
    let meta = meta("absent");
    let binding = prepared(validate_delegated_parent(config.parent().unwrap()).unwrap());
    write_binding_atomically(&meta, &binding).unwrap();
    let before = std::fs::read(binding_path(&meta)).unwrap();
    assert!(!binding.run_root.exists());
    let mut foreign = binding.clone();
    foreign.boot_id = "foreign".into();
    write_binding_atomically(&meta, &foreign).unwrap();
    assert!(matches!(
        reconcile_host_cgroup(&meta, &config, "run"),
        Err(HostCgroupError::Identity(_))
    ));
    assert_eq!(load_host_binding(&meta).unwrap().unwrap(), foreign);
    write_binding_atomically(&meta, &binding).unwrap();
    assert!(reconcile_host_cgroup(&meta, &config, "another-run").is_err());
    let changed =
        HostCgroupConfig::from_parent(Some(binding.delegated_parent.join("different-parent")))
            .unwrap();
    assert!(reconcile_host_cgroup(&meta, &changed, "run").is_err());
    assert_eq!(std::fs::read(binding_path(&meta)).unwrap(), before);
    assert_eq!(
        reconcile_host_cgroup(&meta, &config, "run").unwrap(),
        HostCgroupReconcile::Cleaned
    );
    let retired = load_host_binding(&meta).unwrap().unwrap();
    assert!(retired.prepared && retired.cleaned && retired.empty_confirmed);
    assert_eq!(
        reconcile_host_cgroup(&meta, &config, "run").unwrap(),
        HostCgroupReconcile::AlreadyCleaned
    );
    let scope = bind_host_cgroup(&config, &meta, "run").unwrap().unwrap();
    assert_ne!(scope.run_root, binding.run_root);
    assert!(!load_host_binding(&meta).unwrap().unwrap().prepared);
    // Legacy v1 Bound records retain the normal retirement path.
    let mut legacy = serde_json::to_value(load_host_binding(&meta).unwrap().unwrap()).unwrap();
    legacy["version"] = serde_json::json!(1);
    legacy.as_object_mut().unwrap().remove("prepared");
    std::fs::write(binding_path(&meta), serde_json::to_vec(&legacy).unwrap()).unwrap();
    retire_host_scope(&meta, &scope).unwrap();
    std::fs::remove_dir_all(meta).unwrap();
}

// Only this unit-test binary interprets the helper environment. Production has
// no environment-driven checkpoint/failure behavior.
#[test]
fn host_cgroup_prepared_owner() {
    let Some(meta) = std::env::var_os("QCG_TEST_PREPARED_META") else {
        return;
    };
    let meta = Utf8PathBuf::from_path_buf(meta.into()).unwrap();
    let config = config().expect("parent test requires delegation");
    bind_host_cgroup_inner(&config, &meta, "run", || {
        let binding = load_host_binding(&meta).unwrap().unwrap();
        assert!(binding.prepared && !binding.cleaned && !binding.empty_confirmed);
        // Hook runs after empty confirmation, `cgroup.kill` validation, and
        // post-kill empty confirmation, but before Bound publication.
        assert!(binding.scope().is_none(), "Prepared never yields a scope");
        assert!(!read_occupancy(&binding.run_root).unwrap());
        std::fs::write(meta.join("ready"), b"before Bound").unwrap();
        loop {
            std::thread::park();
        }
    })
    .unwrap();
    panic!("owner must be killed before Bound publication");
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sentinel(scope: &HostCgroupScope) -> Process {
    let mut command = Command::new("/bin/sleep");
    command.arg("60");
    apply_scope_to_std_command(&mut command, scope).unwrap();
    Process(command.spawn().unwrap())
}

#[test]
fn host_cgroup_prepared_sigkill_and_error_preserve_candidate_and_sibling() {
    let Some(config) = config() else { return };
    for crash in [false, true] {
        let meta = meta(if crash { "sigkill" } else { "error" });
        if crash {
            let mut owner = Process(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "host_cgroup::prepared_tests::host_cgroup_prepared_owner",
                        "--nocapture",
                    ])
                    .env("QCG_TEST_PREPARED_META", &meta)
                    .spawn()
                    .unwrap(),
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while !meta.join("ready").exists() {
                assert!(
                    Instant::now() < deadline,
                    "owner missed pre-Bound checkpoint"
                );
                assert!(
                    owner.0.try_wait().unwrap().is_none(),
                    "owner failed before checkpoint"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            let before = std::fs::read(binding_path(&meta)).unwrap();
            let root = load_host_binding(&meta).unwrap().unwrap().run_root;
            let identity = std::fs::metadata(&root).unwrap();
            owner.0.kill().unwrap();
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(owner.0.wait().unwrap().signal(), Some(libc::SIGKILL));
            let after = std::fs::metadata(root).unwrap();
            assert_eq!((identity.dev(), identity.ino()), (after.dev(), after.ino()));
            assert_eq!(std::fs::read(binding_path(&meta)).unwrap(), before);
        } else {
            let error = bind_host_cgroup_inner(&config, &meta, "run", || {
                let binding = load_host_binding(&meta).unwrap().unwrap();
                assert!(binding.prepared && !binding.cleaned && !binding.empty_confirmed);
                std::fs::write(
                    meta.join("before-error"),
                    std::fs::read(binding_path(&meta)).unwrap(),
                )
                .unwrap();
                Err(HostCgroupError::bind("injected pre-Bound failure"))
            })
            .unwrap_err();
            assert!(error.to_string().contains("injected pre-Bound failure"));
            assert_eq!(
                std::fs::read(binding_path(&meta)).unwrap(),
                std::fs::read(meta.join("before-error")).unwrap()
            );
        }
        let before = std::fs::read(binding_path(&meta)).unwrap();
        let binding = load_host_binding(&meta).unwrap().unwrap();
        assert!(binding.prepared && !binding.cleaned && !binding.empty_confirmed);
        let identity = std::fs::metadata(&binding.run_root).unwrap();
        // Refuse even an empty, apparently harmless candidate.
        assert!(!read_occupancy(&binding.run_root).unwrap());
        assert!(matches!(
            reconcile_host_cgroup(&meta, &config, "run"),
            Err(HostCgroupError::Identity(_))
        ));
        let sibling_meta = meta.join("sibling");
        let sibling = bind_host_cgroup(&config, &sibling_meta, "sibling")
            .unwrap()
            .unwrap();
        let mut sibling_process = sentinel(&sibling);
        // Prepared yields no usable scope; fixture placement mirrors the path
        // without authority to prove refusal preserves occupants.
        assert!(binding.scope().is_none(), "Prepared never yields a scope");
        let candidate = candidate_scope(&binding);
        let mut candidate_process = sentinel(&candidate);
        let events = std::fs::read(binding.run_root.join("cgroup.events")).unwrap();
        let procs = std::fs::read(binding.run_root.join("cgroup.procs")).unwrap();
        let sibling_before = std::fs::read(binding_path(&sibling_meta)).unwrap();
        let error = reconcile_host_cgroup(&meta, &config, "run").unwrap_err();
        assert!(matches!(error, HostCgroupError::Identity(_)), "{error}");
        assert!(error.to_string().contains("ownership unproven"));
        assert!(retire_host_scope(&meta, &candidate).is_err());
        assert!(bind_host_cgroup(&config, &meta, "run").is_err());
        assert_eq!(std::fs::read(binding_path(&meta)).unwrap(), before);
        let after = std::fs::metadata(&binding.run_root).unwrap();
        assert_eq!((identity.dev(), identity.ino()), (after.dev(), after.ino()));
        assert_eq!(
            std::fs::read(binding.run_root.join("cgroup.events")).unwrap(),
            events
        );
        assert_eq!(
            std::fs::read(binding.run_root.join("cgroup.procs")).unwrap(),
            procs
        );
        assert!(candidate_process.0.try_wait().unwrap().is_none());
        assert!(sibling_process.0.try_wait().unwrap().is_none());
        assert_eq!(
            std::fs::read(binding_path(&sibling_meta)).unwrap(),
            sibling_before
        );
        assert!(sibling.run_root.is_dir());
        drop(candidate_process);
        drop(sibling_process);
        retire_host_scope(&sibling_meta, &sibling).unwrap();
        // Fixture teardown only, not inferred product ownership or a parent sweep.
        std::fs::remove_dir(&binding.run_root).unwrap();
        std::fs::remove_dir_all(meta).unwrap();
    }
}
