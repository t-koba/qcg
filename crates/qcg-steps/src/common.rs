pub mod base64;
pub mod files;
pub mod json;
pub mod require;
pub mod unix_mode;
pub mod zip;

pub(crate) use base64::*;
pub(crate) use files::*;
pub(crate) use json::*;
pub(crate) use require::*;
pub(crate) use unix_mode::*;
pub(crate) use zip::*;

#[cfg(test)]
pub(crate) mod test_helpers {
    use camino::Utf8PathBuf;
    use qcg_contract::{Contract, NodeDef};

    pub(crate) fn test_contract(name: &str) -> (Contract, Utf8PathBuf) {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-steps-package-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        )))
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary package should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "step-package-validation"
version = "0.1.0"
qcg_version = "^0.1"
"#,
        )
        .expect("manifest should be written");
        let contract = Contract::load(&root).expect("test contract should load");
        (contract, root)
    }

    pub(crate) fn write_package_file(root: &Utf8PathBuf, relative: &str, content: &str) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("package parent should be created");
        }
        std::fs::write(path, content).expect("package file should be written");
    }

    pub(crate) fn package_node(node_id: &str, step_type: &str, params: &str) -> NodeDef {
        toml::from_str(&format!(
            "id = \"{node_id}\"\ntype = \"{step_type}\"\n[params]\n{params}"
        ))
        .expect("step node should parse")
    }
}
