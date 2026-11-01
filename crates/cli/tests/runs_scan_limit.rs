use camino::Utf8PathBuf;
use std::process::Command;

fn write_entries(dir: &Utf8PathBuf, count: usize) {
    std::fs::create_dir_all(dir).unwrap();
    for index in 0..count {
        std::fs::create_dir_all(dir.join(format!("entry-{index}"))).unwrap();
    }
}

fn run(arguments: &[&str], target: &Utf8PathBuf) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(arguments)
        .arg(target)
        .env("MAX_DIRECTORY_SCAN_ENTRIES", "1000")
        .env("AUTO_GC", "false")
        .env_remove("PROVIDERS")
        .output()
        .unwrap()
}

#[test]
fn read_paths_honor_the_configured_scan_bound() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-scan-limit-{}", uuid::Uuid::now_v7()));
    let runs = root.join("runs");
    write_entries(&runs, 1_001);
    let generators = root.join("generators");
    write_entries(&generators, 1_001);

    for (arguments, target) in [
        (&["runs", "list", "--runs-dir"][..], &runs),
        (&["runs", "costs", "--runs-dir"][..], &runs),
        (&["list", "--generators-dir"][..], &generators),
    ] {
        let output = run(arguments, target);
        assert!(
            !output.status.success(),
            "{arguments:?} must refuse a store above the configured bound"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("1000"), "{arguments:?}: {stderr}");
    }

    std::fs::remove_dir_all(root).unwrap();
}
