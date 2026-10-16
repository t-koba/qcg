use std::{path::PathBuf, process::Command};

#[test]
fn invalid_daemon_policy_blocks_container_consumers_before_work_or_bind() {
    let root = std::env::temp_dir().join(format!("qcg-timeout-cli-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let generator = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../generators/generator");

    for name in policy::ContainerTimeouts::ENV_NAMES {
        for (index, value) in ["bad", "0", "3601", "18446744073709551616"]
            .into_iter()
            .enumerate()
        {
            let mut serve = Command::new(env!("CARGO_BIN_EXE_qcg"));
            for key in policy::ContainerTimeouts::ENV_NAMES {
                serve.env_remove(key);
            }
            let runs_dir = root.join(format!("serve-{name}-{index}"));
            serve
                .env(name, value)
                // If resolution moved after bind/address parsing, this
                // invalid address would mask the daemon policy error.
                .args(["serve", "--bind", "not-an-address", "--runs-dir"])
                .arg(&runs_dir);
            let output = serve.output().unwrap();
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(name),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!runs_dir.exists(), "invalid policy must refuse before bind");

            let work = root.join(format!("run-{name}-{index}"));
            std::fs::create_dir_all(&work).unwrap();
            let mut run = Command::new(env!("CARGO_BIN_EXE_qcg"));
            for key in policy::ContainerTimeouts::ENV_NAMES {
                run.env_remove(key);
            }
            let output = run
                .current_dir(&work)
                .env(name, value)
                .env_remove("PROVIDERS")
                .arg("run")
                .arg(&generator)
                .arg("--yes")
                .arg("--output")
                .arg(work.join("out"))
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(name),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                !work.join(".data/runs").exists() && !work.join("out").exists(),
                "invalid policy must refuse before creating run or output directories"
            );
        }
    }

    let mut plan = Command::new(env!("CARGO_BIN_EXE_qcg"));
    for key in policy::ContainerTimeouts::ENV_NAMES {
        plan.env_remove(key);
    }
    for key in [
        "PROVIDERS",
        "PREEMPTION",
        "PREEMPTION_MODE",
        "PREEMPTION_QUEUE_POSITION",
    ] {
        plan.env_remove(key);
    }
    let output = plan
        .current_dir(&root)
        .env("CONTAINER_ADMIN_TIMEOUT_SECS", "0")
        .arg("run")
        .arg(&generator)
        .arg("--plan")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Non-executing service/maintenance commands must not require daemon
    // configuration. They use the documented defaults and retain their own
    // normal lookup behavior instead of failing on an unrelated env value.
    let invalid_admin = "CONTAINER_ADMIN_TIMEOUT_SECS";
    let mut models = Command::new(env!("CARGO_BIN_EXE_qcg"));
    for key in policy::ContainerTimeouts::ENV_NAMES {
        models.env_remove(key);
    }
    for key in [
        "PROVIDERS",
        "PREEMPTION",
        "PREEMPTION_MODE",
        "PREEMPTION_QUEUE_POSITION",
    ] {
        models.env_remove(key);
    }
    let output = models
        .current_dir(&root)
        .env(invalid_admin, "0")
        .args(["models", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    for (name, args) in [
        (
            "invalidate",
            vec![
                "runs",
                "invalidate",
                "missing",
                "--reason",
                "test",
                "--actor-label",
                "test",
            ],
        ),
        ("delete", vec!["runs", "delete", "missing"]),
        ("export", vec!["runs", "export", "missing"]),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
        for key in policy::ContainerTimeouts::ENV_NAMES {
            command.env_remove(key);
        }
        for key in [
            "PROVIDERS",
            "PREEMPTION",
            "PREEMPTION_MODE",
            "PREEMPTION_QUEUE_POSITION",
        ] {
            command.env_remove(key);
        }
        command
            .current_dir(&root)
            .env(invalid_admin, "0")
            .args(args)
            .arg("--runs-dir")
            .arg(root.join(format!("{name}-runs")));
        if name == "export" {
            command.arg("--output").arg(root.join("missing-run.zip"));
        }
        let output = command.output().unwrap();
        assert!(
            !output.status.success(),
            "missing run should retain normal failure"
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains(invalid_admin),
            "maintenance must not reject daemon policy: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Commands that do not construct a container-capable service should not
    // be blocked by unrelated daemon deployment settings.
    let mut new = Command::new(env!("CARGO_BIN_EXE_qcg"));
    for key in policy::ContainerTimeouts::ENV_NAMES {
        new.env_remove(key);
    }
    let output = new
        .env("CONTAINER_ADMIN_TIMEOUT_SECS", "0")
        .arg("new")
        .arg(root.join("new-generator"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    std::fs::remove_dir_all(root).unwrap();
}
