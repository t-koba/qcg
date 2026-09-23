use anyhow::{Context, Result, bail};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{
    SkillDiagnostic, SkillDoc, parse_skill_doc, skill_metadata_value, validate_skill_doc,
};
use serde_json::{Value, json};

#[derive(Debug)]
struct ValidatedSkill {
    path: Utf8PathBuf,
    doc: SkillDoc,
    diagnostics: Vec<SkillDiagnostic>,
}

pub(crate) fn validate_skill(
    path: &Utf8Path,
    library: bool,
    json: bool,
) -> Result<std::process::ExitCode> {
    use std::process::ExitCode;
    let result = if library {
        validate_library(path)
    } else {
        validate_one(path).map(|skill| vec![skill])
    };
    match result {
        Ok(skills) => {
            print_skills(&skills, json)?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "valid": false,
                        "error": format!("{error:#}"),
                    }))?
                );
                // Report through the return code, never process::exit:
                // the library never terminates the process, and the CLI
                // reports machine-readable verdicts through ExitCode.
                Ok(ExitCode::FAILURE)
            } else {
                Err(error)
            }
        }
    }
}

fn validate_library(root: &Utf8Path) -> Result<Vec<ValidatedSkill>> {
    if !root.is_dir() {
        bail!("skill library `{root}` must be a directory");
    }
    let entries = std::fs::read_dir(root)
        .with_context(|| format!("skill library `{root}` cannot be read"))?;
    let mut skills = Vec::new();
    for entry in entries {
        let entry =
            entry.with_context(|| format!("skill library `{root}` cannot read an entry"))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("skill library `{root}` cannot inspect an entry"))?;
        if !file_type.is_dir() {
            continue;
        }
        let child = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|path| anyhow::anyhow!("skill path is not valid UTF-8: {}", path.display()))?;
        if !child.join("SKILL.md").is_file() {
            continue;
        }
        skills.push(validate_one(&child)?);
    }
    if skills.is_empty() {
        bail!("skill library `{root}` contains no skills");
    }
    Ok(skills)
}

fn validate_one(root: &Utf8Path) -> Result<ValidatedSkill> {
    let skill_path = root.join("SKILL.md");
    let source = std::fs::read_to_string(&skill_path)
        .with_context(|| format!("cannot read `{skill_path}`"))?;
    let doc = parse_skill_doc(&source)
        .with_context(|| format!("`{skill_path}` has invalid frontmatter"))?;
    let diagnostics = validate_skill_doc(&doc, root.file_name())
        .with_context(|| format!("`{skill_path}` is invalid"))?;
    Ok(ValidatedSkill {
        path: root.to_path_buf(),
        doc,
        diagnostics,
    })
}

fn print_skills(skills: &[ValidatedSkill], json: bool) -> Result<()> {
    if json {
        let entries: Vec<Value> = skills
            .iter()
            .map(|skill| {
                let mut entry = skill_metadata_value(&skill.doc);
                if let Some(object) = entry.as_object_mut() {
                    object.insert("path".into(), json!(skill.path));
                    if !skill.diagnostics.is_empty() {
                        object.insert("diagnostics".into(), json!(skill.diagnostics));
                    }
                }
                entry
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "valid": true,
                "skills": entries,
            }))?
        );
        return Ok(());
    }
    for skill in skills {
        println!(
            "valid: {} ({})",
            skill.doc.name,
            skill.path.join("SKILL.md")
        );
        for diagnostic in &skill.diagnostics {
            println!("warning: {}", diagnostic.message);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-cli-skill-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path should be UTF-8")
    }

    #[test]
    fn validates_a_single_skill_directory() {
        let root = temp_dir("single");
        std::fs::create_dir_all(root.join("demo")).expect("skill directory should be created");
        std::fs::write(
            root.join("demo/SKILL.md"),
            "---\nname: demo\ndescription: Demo skill.\n---\n\nBody.\n",
        )
        .expect("skill file should be written");
        let skills = validate_one(&root.join("demo")).expect("valid skill should pass");
        assert_eq!(skills.doc.name, "demo");
        assert!(skills.diagnostics.is_empty());
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn rejects_a_skill_without_frontmatter() {
        let root = temp_dir("no-frontmatter");
        std::fs::create_dir_all(root.join("demo")).expect("skill directory should be created");
        std::fs::write(root.join("demo/SKILL.md"), "# Plain\n")
            .expect("skill file should be written");
        let error = validate_one(&root.join("demo")).expect_err("plain markdown must fail");
        assert!(error.to_string().contains("invalid frontmatter"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn rejects_a_skill_without_description() {
        let root = temp_dir("no-description");
        std::fs::create_dir_all(root.join("demo")).expect("skill directory should be created");
        std::fs::write(root.join("demo/SKILL.md"), "---\nname: demo\n---\n")
            .expect("skill file should be written");
        let error = validate_one(&root.join("demo")).expect_err("missing description must fail");
        assert!(error.to_string().contains("description"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn validates_a_library_and_reports_soft_diagnostics() {
        let root = temp_dir("library");
        std::fs::create_dir_all(root.join("skills/alpha"))
            .expect("skill directory should be created");
        std::fs::create_dir_all(root.join("skills/beta"))
            .expect("skill directory should be created");
        std::fs::write(
            root.join("skills/alpha/SKILL.md"),
            "---\nname: Alpha\ndescription: Alpha skill.\n---\n",
        )
        .expect("skill file should be written");
        std::fs::write(
            root.join("skills/beta/SKILL.md"),
            "---\nname: beta\ndescription: Beta skill.\n---\n",
        )
        .expect("skill file should be written");
        let skills = validate_library(&root.join("skills")).expect("library should validate");
        assert_eq!(skills.len(), 2);
        assert!(
            skills.iter().any(|skill| !skill.diagnostics.is_empty()),
            "name case mismatch should be reported as a diagnostic"
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn rejects_an_empty_library() {
        let root = temp_dir("empty-library");
        std::fs::create_dir_all(root.join("skills")).expect("library directory should be created");
        let error = validate_library(&root.join("skills")).expect_err("empty library must fail");
        assert!(error.to_string().contains("contains no skills"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }
}
