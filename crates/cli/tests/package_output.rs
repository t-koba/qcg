use std::process::{Command, Output};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("qcg-package-output-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(root.join("generator")).unwrap();
        std::fs::write(
            root.join("generator/qcg.toml"),
            "[generator]\nid = \"output\"\nname = \"Output\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        Self(root)
    }

    fn package(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_qcg"))
            .current_dir(&self.0)
            .arg("package")
            .args(args)
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
fn package_publishes_default_and_explicit_bare_output() {
    let fixture = Fixture::new();
    for (args, name) in [
        (vec!["generator"], "generator.pkg"),
        (vec!["generator", "-o", "custom.pkg"], "custom.pkg"),
    ] {
        let output = fixture.package(&args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        let mut archive =
            zip::ZipArchive::new(std::fs::File::open(fixture.0.join(name)).unwrap()).unwrap();
        for entry in ["qcg.toml", "SBOM.spdx.json", "PROVENANCE.intoto.json"] {
            assert!(archive.by_name(entry).is_ok(), "missing {entry}");
        }
    }
    assert!(std::fs::read_dir(&fixture.0).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".part-")
    }));
}

#[test]
fn package_bare_output_inside_input_is_refused_without_replacement() {
    let fixture = Fixture::new();
    std::fs::write(fixture.0.join("generator/generator.pkg"), b"unchanged").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .current_dir(fixture.0.join("generator"))
        .args(["package", "."])
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("outside the package input directory")
    );
    assert_eq!(
        std::fs::read(fixture.0.join("generator/generator.pkg")).unwrap(),
        b"unchanged"
    );
}

#[cfg(unix)]
#[test]
fn package_bare_output_symlink_is_refused_without_following() {
    let fixture = Fixture::new();
    std::fs::write(fixture.0.join("target"), b"unchanged").unwrap();
    std::os::unix::fs::symlink("target", fixture.0.join("generator.pkg")).unwrap();
    let output = fixture.package(&["generator"]);
    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("symbolic link"));
    assert_eq!(
        std::fs::read(fixture.0.join("target")).unwrap(),
        b"unchanged"
    );
    assert!(
        std::fs::symlink_metadata(fixture.0.join("generator.pkg"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[cfg(unix)]
#[test]
fn package_default_output_works_from_non_utf8_current_directory() {
    use std::os::unix::ffi::OsStrExt as _;

    let fixture = Fixture::new();
    let current_dir = fixture.0.join(std::ffi::OsStr::from_bytes(b"cwd-\xff"));
    std::fs::create_dir(&current_dir).unwrap();
    let input = fixture.0.join("generator");
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .current_dir(&current_dir)
        .args(["package", input.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(current_dir.join("generator.pkg").is_file());
}
