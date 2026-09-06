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
    if !(0o600..=0o777).contains(&mode) || mode & !0o777 != 0 {
        return Err(StepError::failed(
            &node.id,
            "unix_mode must be between 0600 and 0777 without special bits",
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

pub(crate) fn apply_unix_mode(path: &camino::Utf8Path, mode: Option<u32>) -> Result<(), String> {
    let Some(mode) = mode else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .map_err(|error| error.to_string())?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Err("unix_mode is unsupported on non-Unix platforms".into())
    }
}
