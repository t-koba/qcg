use qcg_contract::NodeDef;
use qcg_engine::StepError;

pub(crate) fn validate_unix_mode_template(
    node: &NodeDef,
    mode: Option<&str>,
) -> Result<(), StepError> {
    let Some(mode) = mode else {
        return Ok(());
    };
    if mode.contains("{{") || mode.contains("{%") {
        return Ok(());
    }
    parse_unix_mode(node, Some(mode)).map(|_| ())
}

pub(crate) fn parse_unix_mode(
    node: &NodeDef,
    mode: Option<&str>,
) -> Result<Option<u32>, StepError> {
    let Some(mode) = mode else {
        return Ok(None);
    };
    let bytes = mode.as_bytes();
    if bytes.len() != 4
        || bytes[0] != b'0'
        || !bytes[1..].iter().all(|byte| (b'0'..=b'7').contains(byte))
    {
        return Err(StepError::failed(
            &node.id,
            "unix_mode must be a canonical four-digit octal string between 0600 and 0777",
        ));
    }
    let mode = u32::from_str_radix(
        std::str::from_utf8(&bytes[1..]).expect("validated ASCII octal bytes"),
        8,
    )
    .map_err(|_| StepError::failed(&node.id, "unix_mode is not valid octal"))?;
    // World-writable modes are refused at validation: workspace writes must
    // never become world-writable (E15). 0600..=0755 without `o+w` covers
    // the legitimate range; 0777-style modes are rejected, not masked.
    if !(0o600..=0o777).contains(&mode) || mode & !0o777 != 0 || mode & 0o002 != 0 {
        return Err(StepError::failed(
            &node.id,
            "unix_mode must be between 0600 and 0777 without world-writable bit and without special bits",
        ));
    }
    if !cfg!(unix) {
        return Err(StepError::failed(
            &node.id,
            "unix_mode is unsupported on non-Unix platforms",
        ));
    }
    Ok(Some(mode))
}

#[cfg(test)]
mod tests {
    use super::parse_unix_mode;
    use qcg_contract::NodeDef;
    fn node() -> NodeDef {
        serde_json::from_value(serde_json::json!({"id": "n", "type": "write"})).expect("node")
    }
    #[test]
    fn world_writable_modes_are_refused() {
        // E15: 0777-style modes never stage world-writable.
        let node = node();
        assert!(parse_unix_mode(&node, Some("0777")).is_err());
        assert!(parse_unix_mode(&node, Some("0666")).is_err());
        #[cfg(unix)]
        {
            assert!(parse_unix_mode(&node, Some("0755")).is_ok());
            assert!(parse_unix_mode(&node, Some("0644")).is_ok());
        }
        #[cfg(not(unix))]
        {
            // POSIX bits cannot be honored here: every explicit mode
            // fails closed instead of staging with a guessed mode.
            assert!(parse_unix_mode(&node, Some("0755")).is_err());
            assert!(parse_unix_mode(&node, Some("0644")).is_err());
        }
    }
}
