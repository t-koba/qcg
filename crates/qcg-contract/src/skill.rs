use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SkillDoc {
    pub name: String,
    pub description: String,
    pub instructions: String,
}

pub fn parse_skill_doc(source: &str) -> SkillDoc {
    let normalized = source.replace("\r\n", "\n");
    let Some(rest) = normalized.strip_prefix("---\n") else {
        return SkillDoc {
            instructions: normalized.trim().to_string(),
            ..SkillDoc::default()
        };
    };
    let Some(end) = rest.find("\n---\n") else {
        return SkillDoc {
            instructions: normalized.trim().to_string(),
            ..SkillDoc::default()
        };
    };
    let meta = &rest[..end];
    let instructions = rest[end + "\n---\n".len()..].trim().to_string();
    SkillDoc {
        name: front_matter_value(meta, "name").unwrap_or_default(),
        description: front_matter_value(meta, "description").unwrap_or_default(),
        instructions,
    }
}

pub fn skill_doc_to_markdown(skill: &SkillDoc) -> String {
    format!(
        "---\nname: {}\ndescription: {}\n---\n\n{}\n",
        quote_front_matter(&skill.name),
        quote_front_matter(&skill.description),
        skill.instructions.trim()
    )
}

fn front_matter_value(meta: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}:");
    meta.lines()
        .find_map(|line| line.trim().strip_prefix(&prefix).map(parse_scalar))
}

fn parse_scalar(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn quote_front_matter(value: &str) -> String {
    serde_json::to_string(value).expect("front matter string must serialize")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_skill_front_matter() {
        let skill = parse_skill_doc("---\nname: demo\ndescription: Demo skill\n---\n\nDo this.\n");
        assert_eq!(skill.name, "demo");
        assert_eq!(skill.description, "Demo skill");
        assert_eq!(skill.instructions, "Do this.");
    }

    #[test]
    fn preserves_plain_markdown_as_instructions() {
        let skill = parse_skill_doc("# Plain\n\nNo metadata.");
        assert_eq!(skill.name, "");
        assert!(skill.instructions.contains("No metadata."));
    }
}
