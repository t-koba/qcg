use contract::ExpectDef;
use engine::StepError;
use model::Finding;
use policy::string_schema;
use serde_json::{Value, json};

pub(crate) fn check_expect_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "exit_code": { "type": "integer" },
            "exit_code_in": { "type": "array", "items": { "type": "integer" } },
            "stdout_contains": string_schema(),
            "stderr_contains": string_schema(),
            "stdout_matches": string_schema(),
        }
    })
}

/// Append stdout/stderr text expectation findings in their established order.
/// `location` controls finding attribution; `node_id` attributes regex errors.
/// Regex compilation remains at execution time, after both `contains` checks.
pub(crate) fn append_output_expect_findings(
    expect: Option<&ExpectDef>,
    stdout: &str,
    stderr: &str,
    location: Option<&str>,
    node_id: &str,
    findings: &mut Vec<Finding>,
) -> Result<(), StepError> {
    let Some(expect) = expect else {
        return Ok(());
    };
    for (stream, needle, output) in [
        ("stdout", expect.stdout_contains.as_deref(), stdout),
        ("stderr", expect.stderr_contains.as_deref(), stderr),
    ] {
        if let Some(needle) = needle
            && !output.contains(needle)
        {
            findings.push(Finding::error(
                format!("{stream} did not contain `{needle}`"),
                location.map(str::to_owned),
                Some(output.to_owned()),
            ));
        }
    }
    if let Some(pattern) = &expect.stdout_matches {
        let expression = regex::Regex::new(pattern).map_err(|error| {
            StepError::failed(
                node_id,
                format!("invalid expect.stdout_matches regex: {error}"),
            )
        })?;
        if !expression.is_match(stdout) {
            findings.push(Finding::error(
                format!("stdout did not match `{pattern}`"),
                location.map(str::to_owned),
                Some(stdout.to_owned()),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect(
        stdout_contains: Option<&str>,
        stderr_contains: Option<&str>,
        stdout_matches: Option<&str>,
    ) -> ExpectDef {
        ExpectDef {
            exit_code: None,
            exit_code_in: Vec::new(),
            stdout_contains: stdout_contains.map(str::to_owned),
            stderr_contains: stderr_contains.map(str::to_owned),
            stdout_matches: stdout_matches.map(str::to_owned),
        }
    }

    fn evaluate(
        expect: Option<&ExpectDef>,
        stdout: &str,
        stderr: &str,
        location: Option<&str>,
        node_id: &str,
    ) -> Result<Vec<Finding>, StepError> {
        let mut findings = Vec::new();
        append_output_expect_findings(expect, stdout, stderr, location, node_id, &mut findings)?;
        Ok(findings)
    }

    #[test]
    fn check_expect_schema_contains_all_supported_fields() {
        let schema = check_expect_schema();
        assert_eq!(schema["type"], "object");
        let properties = schema["properties"].as_object().unwrap();
        assert_eq!(properties.len(), 5);
        assert_eq!(properties["exit_code"]["type"], "integer");
        assert_eq!(properties["exit_code_in"]["items"]["type"], "integer");
        for field in ["stdout_contains", "stderr_contains", "stdout_matches"] {
            assert_eq!(properties[field]["type"], "string");
        }
    }

    #[test]
    fn absent_expect_yields_no_findings() {
        assert!(
            evaluate(None, "out", "err", None, "node")
                .unwrap()
                .is_empty()
        );
        assert!(
            evaluate(None, "out", "err", Some("node"), "node")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn matching_expectations_yield_no_findings() {
        let def = expect(Some("hello"), Some("warn"), Some("h.llo"));
        assert!(
            evaluate(Some(&def), "say hello", "a warn here", None, "node")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn nonmatching_findings_keep_order_diagnostics_and_evidence() {
        let def = expect(Some("missing-out"), Some("missing-err"), Some("missing-re"));
        let findings = evaluate(Some(&def), "stdout-text", "stderr-text", None, "node").unwrap();
        assert_eq!(findings.len(), 3);
        assert_eq!(findings[0].message, "stdout did not contain `missing-out`");
        assert_eq!(findings[0].location, None);
        assert_eq!(findings[0].raw_output.as_deref(), Some("stdout-text"));
        assert_eq!(findings[1].message, "stderr did not contain `missing-err`");
        assert_eq!(findings[1].location, None);
        assert_eq!(findings[1].raw_output.as_deref(), Some("stderr-text"));
        assert_eq!(findings[2].message, "stdout did not match `missing-re`");
        assert_eq!(findings[2].location, None);
        assert_eq!(findings[2].raw_output.as_deref(), Some("stdout-text"));
    }

    #[test]
    fn location_some_attributes_every_finding_to_the_node() {
        let def = expect(Some("missing-out"), Some("missing-err"), Some("missing-re"));
        let findings = evaluate(
            Some(&def),
            "stdout-text",
            "stderr-text",
            Some("container_check"),
            "container_check",
        )
        .unwrap();
        assert_eq!(findings.len(), 3);
        for finding in &findings {
            assert_eq!(finding.location.as_deref(), Some("container_check"));
        }
        assert_eq!(findings[0].raw_output.as_deref(), Some("stdout-text"));
        assert_eq!(findings[1].raw_output.as_deref(), Some("stderr-text"));
        assert_eq!(findings[2].raw_output.as_deref(), Some("stdout-text"));
    }

    #[test]
    fn empty_needles_and_pattern_match_everything() {
        let def = expect(Some(""), Some(""), Some(""));
        assert!(
            evaluate(Some(&def), "anything", "anything", None, "node")
                .unwrap()
                .is_empty()
        );
        assert!(
            evaluate(
                Some(&def),
                "",
                "",
                Some("container_check"),
                "container_check",
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn invalid_regex_fails_after_contains_findings_with_node_attribution() {
        let def = expect(Some("missing-out"), Some("missing-err"), Some("[unclosed"));
        for (location, node_id) in [
            (None, "node-7"),
            (Some("container_check"), "container_check"),
        ] {
            let mut findings = Vec::new();
            let error = append_output_expect_findings(
                Some(&def),
                "out",
                "err",
                location,
                node_id,
                &mut findings,
            )
            .expect_err("invalid regex must fail");
            assert_eq!(findings.len(), 2);
            assert_eq!(findings[0].message, "stdout did not contain `missing-out`");
            assert_eq!(findings[1].message, "stderr did not contain `missing-err`");
            match &error {
                StepError::Failed { node, message } => {
                    assert_eq!(node, node_id);
                    assert!(
                        message.starts_with("invalid expect.stdout_matches regex: "),
                        "unexpected message: {message}"
                    );
                }
                other => panic!("expected StepError::Failed, got {other:?}"),
            }
        }
    }
}
