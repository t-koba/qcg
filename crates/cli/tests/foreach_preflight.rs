use camino::Utf8PathBuf;
use std::process::{Command, Output};

struct Fixture(Utf8PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-foreach-preflight-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(root.join("generator")).unwrap();
        Self(root)
    }

    fn manifest(&self, parallel: usize, items: &str) {
        std::fs::write(
            self.0.join("generator/qcg.toml"),
            format!(
                r#"
[generator]
id = "foreach-preflight"
name = "Foreach preflight"
version = "0.1.0"
description = "Reject unsafe foreach before writes"
[permissions]
fs_read = []
network = []
fs_write = ["workspace"]
side_effects = "allowed"
containers = {{ enabled = false }}
[[permissions.commands]]
bin = "echo"
args = ["child"]
purpose = "test command child"
isolation = "trusted_host"
[[flow]]
id = "before"
type = "write"
[flow.params]
content = "written"
output_file = "before.txt"
[[flow]]
id = "each"
type = "foreach"
[flow.params]
items = "{items}"
subflow = "body"
max_iterations = 10
parallel = {parallel}
[[blocks.body]]
id = "command_child"
type = "command"
[blocks.body.params]
command = ["echo", "child"]
"#
            ),
        )
        .unwrap();
    }

    fn cli(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_qcg"))
            .current_dir(&self.0)
            .args(args)
            .env_remove("PROVIDERS")
            .env("AUTO_GC", "0")
            .output()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn declared_parallel_command_foreach_is_rejected_before_side_effects() {
    let fixture = Fixture::new();
    // Even zero/one eventual iterations cannot make an unsafe declaration valid.
    for items in ["['a', 'b']", "['a']", "[]"] {
        fixture.manifest(2, items);
        let package = fixture.cli(&["package", "generator", "--output", "./generator.pkg"]);
        assert!(package.status.success(), "{package:?}");
        for args in [
            vec!["validate", "generator"],
            vec!["validate", "generator", "--json"],
            vec![
                "install",
                "./generator.pkg",
                "--generators-dir",
                "installed",
                "--yes",
            ],
            vec!["run", "generator", "--output", "out", "--yes"],
        ] {
            let output = fixture.cli(&args);
            assert!(!output.status.success(), "{args:?}: {output:?}");
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                text.contains("command_child") && text.contains("not parallel-safe"),
                "{args:?}: {text}"
            );
            assert!(!fixture.0.join("out/before.txt").exists());
            assert!(!fixture.0.join("installed/foreach-preflight").exists());
        }
    }
}

#[test]
fn one_item_serial_command_foreach_remains_valid() {
    let fixture = Fixture::new();
    fixture.manifest(1, "['a']");
    let validated = fixture.cli(&["validate", "generator"]);
    assert!(validated.status.success(), "{validated:?}");
    let output = fixture.cli(&["run", "generator", "--output", "out", "--yes"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        std::fs::read_to_string(fixture.0.join("out/before.txt")).unwrap(),
        "written"
    );
}
