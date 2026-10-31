pub mod base64;
pub mod files;
pub mod json;
mod output_expect;
pub mod require;
pub mod unix_mode;
pub mod zip;

pub(crate) use base64::*;
pub(crate) use files::*;
pub(crate) use json::*;
pub(crate) use output_expect::*;
pub(crate) use require::*;
pub(crate) use unix_mode::*;
pub(crate) use zip::*;

#[cfg(test)]
pub(crate) mod test_helpers {
    use camino::Utf8PathBuf;
    use contract::{Contract, NodeDef};

    pub(crate) fn test_contract(name: &str) -> (Contract, Utf8PathBuf) {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "steps-package-{name}-{}-{}",
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
            root.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "step-package-validation"
version = "0.1.0"
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

#[cfg(test)]
pub(crate) mod json_test_helpers {
    use super::test_helpers::test_contract;
    use contract::ValueBag;
    use engine::{JournalWriter, RunContext, StepContext};

    pub(crate) struct Harness {
        pub run: RunContext,
        journal: JournalWriter,
        vars: ValueBag,
    }

    impl Harness {
        pub fn new(name: &str) -> Self {
            let (mut contract, root) = test_contract(name);
            contract.manifest.permissions.fs_read = vec!["workspace".into()];
            contract.manifest.permissions.fs_write = vec!["workspace".into()];
            contract.manifest.permissions.network = vec!["127.0.0.1".into()];
            std::fs::create_dir(root.join("workspace")).unwrap();
            let run = RunContext::for_tests(
                name.into(),
                root.join("workspace"),
                root.join("meta"),
                contract,
                Default::default(),
                Default::default(),
            )
            .unwrap();
            let journal =
                JournalWriter::create(&root.join("journal.jsonl"), name, false, None).unwrap();
            Self {
                run,
                journal,
                vars: ValueBag::with_inputs(Default::default()),
            }
        }

        pub fn context(&mut self) -> StepContext<'_> {
            StepContext {
                run: &self.run,
                journal: &self.journal,
                vars: &mut self.vars,
                llm: None,
            }
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.run.contract.root);
        }
    }
}
