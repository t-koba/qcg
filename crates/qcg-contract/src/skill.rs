use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const SKILL_NAME_MAX_CHARS: usize = 64;
pub const SKILL_DESCRIPTION_MAX_CHARS: usize = 1024;
pub const SKILL_COMPATIBILITY_MAX_CHARS: usize = 500;

/// Directory names that are never treated as skill entries when scanning a
/// skill library root.
pub fn is_skill_library_entry(name: &str) -> bool {
    !matches!(name, ".git" | "node_modules")
}

/// Parsed `SKILL.md` document following the Agent Skills specification.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SkillDoc {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<String>,
    pub instructions: String,
    /// Lenient spec violations found while parsing or validating. Hard
    /// violations fail; these remain visible to the operator.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<SkillDiagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SkillDiagnosticSeverity {
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SkillDiagnostic {
    pub severity: SkillDiagnosticSeverity,
    pub message: String,
}

impl SkillDiagnostic {
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            severity: SkillDiagnosticSeverity::Warning,
            message: message.into(),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SkillParseError {
    #[error("SKILL.md frontmatter must start with `---` on the first line")]
    MissingFrontMatter,
    #[error("SKILL.md frontmatter is missing its closing `---` delimiter")]
    UnterminatedFrontMatter,
    #[error("SKILL.md frontmatter line {line} is invalid: {message}")]
    InvalidLine { line: usize, message: String },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SkillValidationError {
    #[error("SKILL.md frontmatter `name` is required and must not be empty")]
    MissingName,
    #[error("SKILL.md frontmatter `description` is required and must not be empty")]
    MissingDescription,
}

/// Parse Agent Skills `SKILL.md` frontmatter and body.
///
/// The parser implements the YAML subset that real skill frontmatter uses:
/// plain, single-quoted, and double-quoted scalars, literal (`|`) and folded
/// (`>`) block scalars with chomping, and a flat `metadata` string map.
/// Anchors, aliases, flow collections, and multi-document files are rejected
/// as invalid lines instead of being silently misread. Unknown top-level keys
/// are rejected (fail-closed): silently ignoring them would accept
/// misspelled required fields as valid skills.
pub fn parse_skill_doc(source: &str) -> Result<SkillDoc, SkillParseError> {
    let source = match source.strip_prefix('\u{feff}') {
        Some(rest) => rest,
        None => source,
    };
    let normalized = source.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();
    let Some(first) = lines.first() else {
        return Err(SkillParseError::MissingFrontMatter);
    };
    if first.trim_end() != "---" {
        return Err(SkillParseError::MissingFrontMatter);
    }

    let mut front: Vec<(usize, &str)> = Vec::new();
    let mut body: Vec<&str> = Vec::new();
    let mut closed = false;
    for (index, line) in lines.iter().enumerate().skip(1) {
        if !closed {
            if line.trim_end() == "---" {
                closed = true;
                continue;
            }
            front.push((index + 1, line));
        } else {
            body.push(line);
        }
    }
    if !closed {
        return Err(SkillParseError::UnterminatedFrontMatter);
    }

    let mut diagnostics = Vec::new();
    let fields = parse_front_matter(&front, &mut diagnostics)?;
    // Empty strings fail explicitly here, not via a default fallback:
    // a missing or blank name/description is a hard parse violation.
    let Some(name) = fields.name else {
        return Err(SkillParseError::InvalidLine {
            line: 1,
            message: "SKILL.md frontmatter `name` is required and must not be empty".into(),
        });
    };
    if name.trim().is_empty() {
        return Err(SkillParseError::InvalidLine {
            line: 1,
            message: "SKILL.md frontmatter `name` must not be empty".into(),
        });
    }
    let Some(description) = fields.description else {
        return Err(SkillParseError::InvalidLine {
            line: 1,
            message: "SKILL.md frontmatter `description` is required and must not be empty".into(),
        });
    };
    if description.trim().is_empty() {
        return Err(SkillParseError::InvalidLine {
            line: 1,
            message: "SKILL.md frontmatter `description` must not be empty".into(),
        });
    }
    Ok(SkillDoc {
        name,
        description,
        license: fields.license,
        compatibility: fields.compatibility,
        metadata: fields.metadata,
        allowed_tools: fields.allowed_tools,
        instructions: body.join("\n").trim().to_string(),
        diagnostics,
    })
}

/// Validate a parsed skill against the Agent Skills specification.
///
/// Hard violations return an error: the specification requires a name and a
/// description, and a missing description makes the skill undiscoverable.
/// Soft violations (lengths, character set, directory mismatch) are returned
/// as diagnostics so clients can warn without refusing the skill.
pub fn validate_skill_doc(
    doc: &SkillDoc,
    expected_name: Option<&str>,
) -> Result<Vec<SkillDiagnostic>, SkillValidationError> {
    if doc.name.trim().is_empty() {
        return Err(SkillValidationError::MissingName);
    }
    if doc.description.trim().is_empty() {
        return Err(SkillValidationError::MissingDescription);
    }
    let mut diagnostics = doc.diagnostics.clone();
    if doc.name.chars().count() > SKILL_NAME_MAX_CHARS {
        diagnostics.push(SkillDiagnostic::warning(format!(
            "skill name `{}` exceeds {SKILL_NAME_MAX_CHARS} characters",
            doc.name
        )));
    }
    if !doc
        .name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
    {
        diagnostics.push(SkillDiagnostic::warning(format!(
            "skill name `{}` may only contain lowercase letters, digits, and hyphens",
            doc.name
        )));
    }
    if doc.name.starts_with('-') || doc.name.ends_with('-') || doc.name.contains("--") {
        diagnostics.push(SkillDiagnostic::warning(format!(
            "skill name `{}` must not start or end with a hyphen or contain consecutive hyphens",
            doc.name
        )));
    }
    if let Some(expected) = expected_name
        && doc.name != expected
    {
        diagnostics.push(SkillDiagnostic::warning(format!(
            "skill name `{}` does not match its directory `{expected}`",
            doc.name
        )));
    }
    if doc.description.chars().count() > SKILL_DESCRIPTION_MAX_CHARS {
        diagnostics.push(SkillDiagnostic::warning(format!(
            "skill description exceeds {SKILL_DESCRIPTION_MAX_CHARS} characters"
        )));
    }
    if let Some(compatibility) = &doc.compatibility
        && compatibility.chars().count() > SKILL_COMPATIBILITY_MAX_CHARS
    {
        diagnostics.push(SkillDiagnostic::warning(format!(
            "skill compatibility exceeds {SKILL_COMPATIBILITY_MAX_CHARS} characters"
        )));
    }
    Ok(diagnostics)
}

/// Canonical model-facing metadata for a parsed skill.
pub fn skill_metadata_value(doc: &SkillDoc) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert("name".into(), serde_json::Value::String(doc.name.clone()));
    value.insert(
        "description".into(),
        serde_json::Value::String(doc.description.clone()),
    );
    if let Some(license) = &doc.license {
        value.insert("license".into(), serde_json::Value::String(license.clone()));
    }
    if let Some(compatibility) = &doc.compatibility {
        value.insert(
            "compatibility".into(),
            serde_json::Value::String(compatibility.clone()),
        );
    }
    if !doc.metadata.is_empty() {
        // Built without serialization: string maps cannot fail to convert,
        // so no expect is needed.
        value.insert(
            "metadata".into(),
            serde_json::Value::Object(
                doc.metadata
                    .iter()
                    .map(|(key, val)| (key.clone(), serde_json::Value::String(val.clone())))
                    .collect(),
            ),
        );
    }
    if let Some(allowed_tools) = &doc.allowed_tools {
        value.insert(
            "allowed_tools".into(),
            serde_json::Value::String(allowed_tools.clone()),
        );
    }
    if !doc.diagnostics.is_empty() {
        // Built without serialization for the same reason: severity and
        // message are plain strings by construction. The match has no
        // wildcard so a future severity variant fails compilation here
        // instead of silently diverging from the serde shape.
        value.insert(
            "diagnostics".into(),
            serde_json::Value::Array(
                doc.diagnostics
                    .iter()
                    .map(|diagnostic| {
                        serde_json::json!({
                            "severity": match diagnostic.severity {
                                SkillDiagnosticSeverity::Warning => "warning",
                            },
                            "message": diagnostic.message,
                        })
                    })
                    .collect(),
            ),
        );
    }
    serde_json::Value::Object(value)
}

#[derive(Debug, Default)]
struct FrontMatterFields {
    name: Option<String>,
    description: Option<String>,
    license: Option<String>,
    compatibility: Option<String>,
    metadata: BTreeMap<String, String>,
    allowed_tools: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chomping {
    Clip,
    Strip,
    Keep,
}

fn parse_front_matter(
    front: &[(usize, &str)],
    diagnostics: &mut Vec<SkillDiagnostic>,
) -> Result<FrontMatterFields, SkillParseError> {
    let mut fields = FrontMatterFields::default();
    let mut seen = BTreeSet::new();
    let mut index = 0;
    while index < front.len() {
        let (line_number, line) = front[index];
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            index += 1;
            continue;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(SkillParseError::InvalidLine {
                line: line_number,
                message: "unexpected indented line outside a nested block".into(),
            });
        }
        let Some((key, value)) = line.split_once(':') else {
            return Err(SkillParseError::InvalidLine {
                line: line_number,
                message: "expected a `key: value` pair".into(),
            });
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(SkillParseError::InvalidLine {
                line: line_number,
                message: "frontmatter key is empty".into(),
            });
        }
        if !seen.insert(key.to_string()) {
            diagnostics.push(SkillDiagnostic::warning(format!(
                "duplicate frontmatter key `{key}`; the later value wins"
            )));
        }
        let value = value.trim_start();
        if let Some((style, chomping, indent)) =
            block_indicator(value).map_err(|message| SkillParseError::InvalidLine {
                line: line_number,
                message,
            })?
        {
            let (scalar, next) =
                read_block_scalar(front, index + 1, style, chomping, indent, line_number)?;
            assign_field(&mut fields, key, scalar, line_number)?;
            index = next;
            continue;
        }
        if key == "metadata" && value.trim().is_empty() {
            let (map, next) = read_metadata_map(front, index + 1, diagnostics);
            fields.metadata.extend(map);
            index = next;
            continue;
        }
        let quoted = value.starts_with('"') || value.starts_with('\'');
        let scalar = parse_scalar(value).map_err(|message| SkillParseError::InvalidLine {
            line: line_number,
            message,
        })?;
        if value.trim().is_empty() {
            assign_field(&mut fields, key, scalar, line_number)?;
            index = skip_indented_block(front, index + 1);
            continue;
        }
        if quoted {
            assign_field(&mut fields, key, scalar, line_number)?;
            index = skip_indented_block(front, index + 1);
            continue;
        }
        let (scalar, next) = read_plain_continuation(front, index + 1, scalar);
        assign_field(&mut fields, key, scalar, line_number)?;
        index = next;
    }
    Ok(fields)
}

fn skip_indented_block(front: &[(usize, &str)], mut cursor: usize) -> usize {
    while let Some((_, line)) = front.get(cursor) {
        if line.starts_with(' ') || line.starts_with('\t') {
            cursor += 1;
            continue;
        }
        if line.trim().is_empty() {
            let mut probe = cursor;
            while probe < front.len() && front[probe].1.trim().is_empty() {
                probe += 1;
            }
            if probe < front.len()
                && (front[probe].1.starts_with(' ') || front[probe].1.starts_with('\t'))
            {
                cursor = probe;
                continue;
            }
        }
        break;
    }
    cursor
}

fn read_plain_continuation(
    front: &[(usize, &str)],
    mut cursor: usize,
    initial: String,
) -> (String, usize) {
    let mut text = initial;
    while let Some((_, line)) = front.get(cursor) {
        if line.trim().is_empty() {
            let mut probe = cursor;
            while probe < front.len() && front[probe].1.trim().is_empty() {
                probe += 1;
            }
            if probe < front.len()
                && (front[probe].1.starts_with(' ') || front[probe].1.starts_with('\t'))
            {
                text.push('\n');
                cursor = probe;
                continue;
            }
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push(' ');
            }
            text.push_str(line.trim());
            cursor += 1;
            continue;
        }
        break;
    }
    (text, cursor)
}

fn assign_field(
    fields: &mut FrontMatterFields,
    key: &str,
    value: String,
    line_number: usize,
) -> Result<(), SkillParseError> {
    match key {
        "name" => fields.name = Some(value),
        "description" => fields.description = Some(value),
        "license" => fields.license = Some(value),
        "compatibility" => fields.compatibility = Some(value),
        "allowed-tools" => fields.allowed_tools = Some(value),
        // Fail-closed on unknown keys: a misspelled required field must not
        // read as a missing field with a default.
        _ => {
            return Err(SkillParseError::InvalidLine {
                line: line_number,
                message: format!("unknown frontmatter key `{key}`"),
            });
        }
    }
    Ok(())
}

fn block_indicator(value: &str) -> Result<Option<(char, Chomping, Option<usize>)>, String> {
    let Some(style) = value.chars().next() else {
        return Ok(None);
    };
    if style != '|' && style != '>' {
        return Ok(None);
    }
    let mut chomping = Chomping::Clip;
    let mut indent = None;
    for ch in value.chars().skip(1) {
        match ch {
            '-' => chomping = Chomping::Strip,
            '+' => chomping = Chomping::Keep,
            '0'..='9' => indent = Some(ch as usize - '0' as usize),
            ' ' | '\t' => break,
            '#' => break,
            other => {
                return Err(format!(
                    "block scalar indicator contains unexpected character `{other}`"
                ));
            }
        }
    }
    Ok(Some((style, chomping, indent)))
}

fn read_block_scalar(
    front: &[(usize, &str)],
    start: usize,
    style: char,
    chomping: Chomping,
    explicit_indent: Option<usize>,
    key_line: usize,
) -> Result<(String, usize), SkillParseError> {
    let mut cursor = start;
    let base = match explicit_indent {
        Some(indent) => indent,
        None => {
            let mut found = None;
            let mut probe = start;
            while probe < front.len() {
                let line = front[probe].1;
                if !line.trim().is_empty() {
                    found =
                        Some(
                            leading_spaces(line).ok_or_else(|| SkillParseError::InvalidLine {
                                line: front[probe].0,
                                message: "tabs cannot be used for block scalar indentation".into(),
                            })?,
                        );
                    break;
                }
                probe += 1;
            }
            match found {
                Some(indent) => indent,
                None => return Ok((String::new(), front.len())),
            }
        }
    };
    if base == 0 {
        return Err(SkillParseError::InvalidLine {
            line: key_line,
            message: "block scalar content must be indented".into(),
        });
    }

    let mut content: Vec<&str> = Vec::new();
    while cursor < front.len() {
        let (line_number, line) = front[cursor];
        if line.trim().is_empty() {
            content.push("");
            cursor += 1;
            continue;
        }
        let indent = leading_spaces(line).ok_or_else(|| SkillParseError::InvalidLine {
            line: line_number,
            message: "tabs cannot be used for block scalar indentation".into(),
        })?;
        if indent < base {
            break;
        }
        content.push(&line[base.min(line.len())..]);
        cursor += 1;
    }

    let raw = if style == '>' {
        fold_lines(&content)
    } else {
        content.join("\n")
    };
    let raw = format!("{raw}\n");
    let scalar = match chomping {
        Chomping::Keep => raw,
        Chomping::Strip => raw.trim_end_matches('\n').to_string(),
        Chomping::Clip => {
            let stripped = raw.trim_end_matches('\n');
            if stripped.is_empty() {
                String::new()
            } else {
                format!("{stripped}\n")
            }
        }
    };
    Ok((scalar, cursor))
}

fn fold_lines(lines: &[&str]) -> String {
    let mut folded = String::new();
    let mut previous_empty = true;
    for line in lines {
        if line.is_empty() {
            folded.push('\n');
            previous_empty = true;
        } else {
            if !previous_empty {
                folded.push(' ');
            }
            folded.push_str(line);
            previous_empty = false;
        }
    }
    folded
}

fn read_metadata_map(
    front: &[(usize, &str)],
    start: usize,
    diagnostics: &mut Vec<SkillDiagnostic>,
) -> (BTreeMap<String, String>, usize) {
    let mut cursor = start;
    let mut base = None;
    while cursor < front.len() {
        let line = front[cursor].1;
        if line.trim().is_empty() {
            cursor += 1;
            continue;
        }
        base = leading_spaces(line);
        break;
    }
    let Some(base) = base else {
        return (BTreeMap::new(), cursor);
    };
    if base == 0 {
        return (BTreeMap::new(), cursor);
    }
    let mut map = BTreeMap::new();
    while cursor < front.len() {
        let (line_number, line) = front[cursor];
        if line.trim().is_empty() {
            cursor += 1;
            continue;
        }
        let Some(indent) = leading_spaces(line) else {
            diagnostics.push(SkillDiagnostic::warning(format!(
                "metadata line {line_number} uses a tab for indentation"
            )));
            cursor += 1;
            continue;
        };
        if indent < base {
            break;
        }
        if indent > base {
            diagnostics.push(SkillDiagnostic::warning(format!(
                "metadata line {line_number} nests deeper than the metadata map; the entry is ignored"
            )));
            cursor += 1;
            continue;
        }
        let body = line.trim_start();
        if body.starts_with('#') {
            cursor += 1;
            continue;
        }
        let Some((key, value)) = body.split_once(':') else {
            diagnostics.push(SkillDiagnostic::warning(format!(
                "metadata line {line_number} is not a `key: value` pair; the entry is ignored"
            )));
            cursor += 1;
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            diagnostics.push(SkillDiagnostic::warning(format!(
                "metadata line {line_number} has an empty key; the entry is ignored"
            )));
            cursor += 1;
            continue;
        }
        match parse_scalar(value) {
            Ok(value) if !value.is_empty() => {
                if map.insert(key.to_string(), value).is_some() {
                    diagnostics.push(SkillDiagnostic::warning(format!(
                        "duplicate metadata key `{key}`; the later value wins"
                    )));
                }
            }
            Ok(_) => diagnostics.push(SkillDiagnostic::warning(format!(
                "metadata key `{key}` has an empty value; nested metadata is not supported and the entry is ignored"
            ))),
            Err(message) => diagnostics.push(SkillDiagnostic::warning(format!(
                "metadata key `{key}` is invalid: {message}; the entry is ignored"
            ))),
        }
        cursor += 1;
    }
    (map, cursor)
}

fn leading_spaces(line: &str) -> Option<usize> {
    let mut spaces = 0;
    for ch in line.chars() {
        match ch {
            ' ' => spaces += 1,
            '\t' => return None,
            _ => break,
        }
    }
    Some(spaces)
}

fn parse_scalar(raw: &str) -> Result<String, String> {
    let value = strip_comment(raw).trim();
    if value.is_empty() {
        return Ok(String::new());
    }
    if let Some(inner) = value.strip_prefix('"') {
        let Some(inner) = inner.strip_suffix('"') else {
            return Err("unterminated double-quoted string".into());
        };
        return unescape_double_quoted(inner);
    }
    if let Some(inner) = value.strip_prefix('\'') {
        let Some(inner) = inner.strip_suffix('\'') else {
            return Err("unterminated single-quoted string".into());
        };
        return Ok(inner.replace("''", "'"));
    }
    Ok(value.to_string())
}

fn strip_comment(raw: &str) -> &str {
    for (index, ch) in raw.char_indices() {
        if ch == '#' && (index == 0 || raw[..index].ends_with(char::is_whitespace)) {
            return &raw[..index];
        }
    }
    raw
}

fn unescape_double_quoted(inner: &str) -> Result<String, String> {
    let mut result = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            result.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err("double-quoted string ends with a dangling escape".into());
        };
        match escaped {
            'n' => result.push('\n'),
            't' => result.push('\t'),
            'r' => result.push('\r'),
            '"' => result.push('"'),
            '\\' => result.push('\\'),
            '0' => result.push('\0'),
            'u' => {
                let mut digits = String::new();
                for _ in 0..4 {
                    let Some(digit) = chars.next() else {
                        return Err("double-quoted string has an incomplete `\\u` escape".into());
                    };
                    digits.push(digit);
                }
                let code = u32::from_str_radix(&digits, 16)
                    .map_err(|_| "double-quoted string has an invalid `\\u` escape".to_string())?;
                let Some(decoded) = char::from_u32(code) else {
                    return Err("double-quoted string has an invalid unicode escape".into());
                };
                result.push(decoded);
            }
            other => {
                result.push('\\');
                result.push(other);
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_skill_front_matter() {
        let skill = parse_skill_doc("---\nname: demo\ndescription: Demo skill\n---\n\nDo this.\n")
            .expect("frontmatter should parse");
        assert_eq!(skill.name, "demo");
        assert_eq!(skill.description, "Demo skill");
        assert_eq!(skill.instructions, "Do this.");
    }

    #[test]
    fn parses_folded_block_description() {
        let source = "---\nname: demo\ndescription: >-\n  Use this skill when:\n  the user asks about PDFs.\n---\n\nBody.\n";
        let skill = parse_skill_doc(source).expect("folded description should parse");
        assert_eq!(
            skill.description,
            "Use this skill when: the user asks about PDFs."
        );
        assert!(skill.instructions.contains("Body."));
    }

    #[test]
    fn parses_literal_block_description_with_clip() {
        let source = "---\nname: demo\ndescription: |\n  line one\n  line two\n---\nBody.\n";
        let skill = parse_skill_doc(source).expect("literal description should parse");
        assert_eq!(skill.description, "line one\nline two\n");
    }

    #[test]
    fn parses_quoted_scalars_and_metadata() {
        let source = "---\nname: \"pdf-processing\"\ndescription: 'Extract: text'\nlicense: Apache-2.0\ncompatibility: Requires git\nmetadata:\n  author: example-org\n  version: \"1.0\"\nallowed-tools: Bash(git:*) Read\n---\nBody.\n";
        let skill = parse_skill_doc(source).expect("quoted frontmatter should parse");
        assert_eq!(skill.name, "pdf-processing");
        assert_eq!(skill.description, "Extract: text");
        assert_eq!(skill.license.as_deref(), Some("Apache-2.0"));
        assert_eq!(skill.compatibility.as_deref(), Some("Requires git"));
        assert_eq!(
            skill.metadata.get("author").map(String::as_str),
            Some("example-org")
        );
        assert_eq!(
            skill.metadata.get("version").map(String::as_str),
            Some("1.0")
        );
        assert_eq!(skill.allowed_tools.as_deref(), Some("Bash(git:*) Read"));
    }

    #[test]
    fn rejects_unknown_front_matter_keys() {
        // Fail-closed: unknown keys are rejected, never silently ignored.
        let source = "---\nname: demo\ndescription: Demo\nunknown-key: value\n---\nBody.\n";
        let error = parse_skill_doc(source).expect_err("unknown keys must be rejected");
        assert!(
            matches!(error, SkillParseError::InvalidLine { .. }),
            "unknown keys must fail as invalid lines: {error:?}"
        );
        let nested = "---\nname: demo\ndescription: Demo\nnested:\n  child: value\n---\nBody.\n";
        parse_skill_doc(nested).expect_err("nested unknown keys must be rejected");
    }

    #[test]
    fn accepts_bom_crlf_and_eof_delimiter() {
        let source = "\u{feff}---\r\nname: demo\r\ndescription: Demo skill\r\n---";
        let skill = parse_skill_doc(source).expect("BOM and CRLF should parse");
        assert_eq!(skill.name, "demo");
        assert!(skill.instructions.is_empty());
    }

    #[test]
    fn accepts_delimiter_trailing_spaces() {
        let source = "---\nname: demo\ndescription: Demo\n---   \nBody.\n";
        let skill = parse_skill_doc(source).expect("trailing spaces should parse");
        assert_eq!(skill.instructions, "Body.");
    }

    #[test]
    fn plain_scalar_keeps_colons_and_drops_comments() {
        let source = "---\nname: demo\ndescription: Use this skill when: the user asks # comment\n---\nBody.\n";
        let skill = parse_skill_doc(source).expect("plain scalar should parse");
        assert_eq!(skill.description, "Use this skill when: the user asks");
    }

    #[test]
    fn reports_duplicate_keys_as_warnings() {
        let source = "---\nname: first\nname: second\ndescription: Demo\n---\nBody.\n";
        let skill = parse_skill_doc(source).expect("duplicate keys should parse leniently");
        assert_eq!(skill.name, "second");
        assert!(
            skill
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("duplicate"))
        );
    }

    #[test]
    fn rejects_missing_front_matter() {
        let error = parse_skill_doc("# Plain\n\nNo metadata.")
            .expect_err("plain markdown must not parse as a skill");
        assert!(matches!(error, SkillParseError::MissingFrontMatter));
    }

    #[test]
    fn rejects_unterminated_front_matter() {
        let error = parse_skill_doc("---\nname: demo\ndescription: Demo\n")
            .expect_err("unterminated frontmatter must fail");
        assert!(matches!(error, SkillParseError::UnterminatedFrontMatter));
    }

    #[test]
    fn rejects_unexpected_indentation() {
        let error =
            parse_skill_doc("---\n  name: demo\n---\n").expect_err("stray indentation must fail");
        assert!(matches!(error, SkillParseError::InvalidLine { .. }));
    }

    #[test]
    fn validation_requires_name_and_description() {
        // Empty strings fail explicitly at parse time (no default fallback).
        let missing_name = parse_skill_doc("---\ndescription: Demo\n---\nBody.\n")
            .expect_err("missing name must fail at parse time");
        assert!(
            matches!(missing_name, SkillParseError::InvalidLine { .. }),
            "{missing_name:?}"
        );
        let missing_description = parse_skill_doc("---\nname: demo\n---\nBody.\n")
            .expect_err("missing description must fail at parse time");
        assert!(
            matches!(missing_description, SkillParseError::InvalidLine { .. }),
            "{missing_description:?}"
        );
        // The validator still guards programmatically built docs with empty
        // strings (defense-in-depth for non-parse construction).
        let empty = SkillDoc {
            name: String::new(),
            description: String::new(),
            instructions: "Body.".into(),
            ..SkillDoc::default()
        };
        assert_eq!(
            validate_skill_doc(&empty, None).unwrap_err(),
            SkillValidationError::MissingName
        );
    }

    #[test]
    fn validation_reports_soft_spec_violations() {
        let source = format!(
            "---\nname: Demo--Skill\ndescription: {}\n---\nBody.\n",
            "x".repeat(SKILL_DESCRIPTION_MAX_CHARS + 1)
        );
        let skill = parse_skill_doc(&source).expect("frontmatter should parse");
        let diagnostics = validate_skill_doc(&skill, Some("demo-skill"))
            .expect("soft violations must not fail validation");
        assert!(diagnostics.iter().any(|d| d.message.contains("hyphen")));
        assert!(diagnostics.iter().any(|d| d.message.contains("lowercase")));
        assert!(diagnostics.iter().any(|d| d.message.contains("directory")));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("description"))
        );
    }

    #[test]
    fn metadata_value_serializes_without_instructions() {
        let source =
            "---\nname: demo\ndescription: Demo\nmetadata:\n  author: qcg\n---\nSecret body.\n";
        let skill = parse_skill_doc(source).expect("frontmatter should parse");
        let value = skill_metadata_value(&skill);
        assert_eq!(value["name"], "demo");
        assert_eq!(value["metadata"]["author"], "qcg");
        assert!(value.get("instructions").is_none());
    }
}
