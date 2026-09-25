use anyhow::{Context, Result};
use camino::Utf8PathBuf;

/// Create a minimal generator skeleton.
///
/// Mechanism only: directory layout plus a valid `qcg.toml`.
/// All policy content (prompts, schemas, budgets, permissions) stays
/// with the author as explicit follow-up edits.
pub(crate) fn new_generator(path: &Utf8PathBuf, id: Option<&str>, force: bool) -> Result<()> {
    let id = match id {
        Some(value) => value.to_string(),
        None => path
            .file_name()
            .map(str::to_string)
            .unwrap_or_else(|| "my-generator".to_string()),
    };
    validate_skeleton_id(&id)?;
    if path.exists() {
        let is_empty = std::fs::read_dir(path)
            .with_context(|| format!("failed to read target `{path}`"))?
            .next()
            .is_none();
        if !is_empty && !force {
            anyhow::bail!(
                "target `{path}` already exists and is not empty; use --force to overwrite files"
            );
        }
    } else {
        std::fs::create_dir_all(path)
            .with_context(|| format!("failed to create target `{path}`"))?;
    }
    for dir in ["prompts", "templates", "schemas"] {
        let dir_path = path.join(dir);
        std::fs::create_dir_all(&dir_path)
            .with_context(|| format!("failed to create `{dir_path}`"))?;
        let keep = dir_path.join(".gitkeep");
        if !keep.exists() {
            std::fs::write(&keep, "").with_context(|| format!("failed to write `{keep}`"))?;
        }
    }
    let manifest = minimal_manifest(&id);
    let manifest_path = path.join("qcg.toml");
    std::fs::write(&manifest_path, manifest)
        .with_context(|| format!("failed to write `{manifest_path}`"))?;
    // Fail closed when the skeleton does not validate.
    let contract = qcg_contract::Contract::load(path)?;
    println!(
        "created {}@{} ({})",
        contract.manifest.generator.id, contract.manifest.generator.version, contract.sha256
    );
    Ok(())
}

fn validate_skeleton_id(id: &str) -> Result<()> {
    // Fail closed before interpolating into TOML: the id lands inside a
    // quoted value in the generated manifest, so anything outside a strict
    // charset (quotes, backslashes, newlines, control characters, or TOML
    // syntax) could break out of the string and alter the skeleton.
    let valid = !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if !valid {
        anyhow::bail!("generator id `{id}` must be 1-64 ASCII letters, digits, `-`, or `_`");
    }
    Ok(())
}

fn minimal_manifest(id: &str) -> String {
    format!(
        r##"[generator]
id = "{id}"
name = "{id}"
version = "0.1.0"
qcg_version = "^0.1"
description = "Minimal generator skeleton"

[[flow]]
id = "emit_readme"
type = "write"

[flow.params]
content = "Hello from {id}\n"
output_file = "README.md"

[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"

[permissions.containers]
enabled = false

[budget]
max_steps = 64

[outputs]
extras = []
"##
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skeleton_id_rejects_unsafe_names() {
        assert!(validate_skeleton_id("ok-name_1").is_ok());
        assert!(validate_skeleton_id("bad:id").is_err());
        assert!(validate_skeleton_id("../escape").is_err());
        assert!(validate_skeleton_id("").is_err());
        // TOML breakout vectors: quotes, backslashes, whitespace,
        // newlines, and control characters must all fail closed.
        for hostile in [
            "a\"b",
            "a\\b",
            "a b",
            "a/b",
            "a\nb",
            "a\rb",
            "a\tb",
            "a\x7fb",
            "日本語",
            ".",
            "..",
        ] {
            assert!(
                validate_skeleton_id(hostile).is_err(),
                "hostile id must fail: {hostile:?}"
            );
        }
        assert!(validate_skeleton_id(&"a".repeat(65)).is_err());
    }

    #[test]
    fn new_generator_creates_valid_skeleton() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temp dir must be UTF-8")
            .join(format!("qcg-new-test-{}", std::process::id()));
        let target = root.join("demo-gen");
        let _ = std::fs::remove_dir_all(&root);
        new_generator(&target, None, false).expect("skeleton creation must succeed");
        let contract = qcg_contract::Contract::load(&target).expect("skeleton must validate");
        assert_eq!(contract.manifest.generator.id, "demo-gen");
        assert!(target.join("prompts").join(".gitkeep").exists());
        assert!(target.join("qcg.toml").exists());
        let _ = std::fs::remove_dir_all(&root);
    }
}
