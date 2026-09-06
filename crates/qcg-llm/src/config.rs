use qcg_policy::MAX_CREDENTIAL_FILE_BYTES;
use qcg_policy::credential_like_name;
use std::collections::BTreeMap;
use std::fs::File;
#[cfg(unix)]
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use url::Url;

pub(crate) fn is_env_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().enumerate().all(|(index, character)| {
            character.is_ascii_uppercase()
                || character == '_'
                || (index > 0 && character.is_ascii_digit())
        })
}

pub(crate) fn is_provider_id(id: &str) -> bool {
    !id.is_empty()
        && id.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        })
}

pub(crate) fn interpolate_env(value: &str) -> Result<String, String> {
    let mut result = String::new();
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        let Some(close_offset) = rest[start..].find('}') else {
            return Err("unclosed `{` environment placeholder".into());
        };
        let end = start + close_offset;
        let name = &rest[start + 1..end];
        if !is_env_name(name) {
            return Err(format!("invalid environment placeholder `{{{name}}}`"));
        }
        match std::env::var(name) {
            Ok(value) => {
                result.push_str(&rest[..start]);
                result.push_str(&value);
            }
            Err(_) => {
                return Err(format!("set `{name}` before running the generator"));
            }
        }
        rest = &rest[end + 1..];
    }
    result.push_str(rest);
    Ok(result)
}

pub(crate) fn normalize_url_placeholders(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        normalized.push_str(&rest[..start]);
        let Some(close_offset) = rest[start..].find('}') else {
            normalized.push_str(&rest[start..]);
            return normalized;
        };
        normalized.push_str("placeholder");
        rest = &rest[start + close_offset + 1..];
    }
    normalized.push_str(rest);
    normalized
}

pub(crate) fn credential_placeholder(value: &str, credential_env: Option<&str>) -> Option<String> {
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        let close_offset = rest[start..].find('}')?;
        let name = &rest[start + 1..start + close_offset];
        if credential_env.is_some_and(|credential_env| name == credential_env)
            || credential_like_name(name)
        {
            return Some(name.to_owned());
        }
        rest = &rest[start + close_offset + 1..];
    }
    None
}

pub(crate) fn validate_query_parameters(
    query: &BTreeMap<String, String>,
    credential_env: Option<&str>,
) -> Result<(), String> {
    for (key, value) in query {
        if credential_env.is_some_and(|credential_env| key == credential_env)
            || credential_like_name(key)
        {
            return Err(format!(
                "query parameter `{key}` must not carry credentials"
            ));
        }
        if let Some(name) = credential_placeholder(value, credential_env) {
            return Err(format!(
                "query parameter `{key}` must not interpolate credential environment variable `{name}`"
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_base_url(raw: &str, requires_credential: bool) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|_| "base_url is not a valid URL".to_owned())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("base_url must use the http or https scheme".into());
    }
    if url.host_str().is_none() {
        return Err("base_url must include a host".into());
    }
    if has_url_userinfo(&url) {
        return Err("base_url must not include userinfo".into());
    }
    if url.query().is_some() {
        return Err("base_url must not include a query".into());
    }
    if url.fragment().is_some() {
        return Err("base_url must not include a fragment".into());
    }
    if requires_credential && url.scheme() == "http" && !is_loopback_host(&url) {
        return Err("credentialed http base_url is only permitted for loopback hosts".into());
    }
    Ok(url)
}

pub(crate) fn read_credential_file(path: &Path) -> Result<String, String> {
    if !path.is_absolute() {
        return Err("path must be absolute".into());
    }
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect `{}`: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("path must be a regular non-symlink file".into());
    }
    if metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        return Err(format!("file exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes"));
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("file permissions must not grant group or other access".into());
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options
            .open(path)
            .map_err(|error| format!("cannot open safely: {error}"))?
    };
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|error| format!("cannot open: {error}"))?;

    read_open_credential_file(file)
}

fn read_open_credential_file(file: File) -> Result<String, String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot inspect after opening: {error}"))?;
    if !metadata.is_file() || metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        return Err(format!(
            "file must be regular and no larger than {MAX_CREDENTIAL_FILE_BYTES} bytes"
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CREDENTIAL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read: {error}"))?;
    if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
        return Err(format!("file exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes"));
    }
    let value = String::from_utf8(bytes).map_err(|_| "file is not valid UTF-8".to_owned())?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value)
        .to_owned();
    if value.is_empty() {
        return Err("file is empty".into());
    }
    Ok(value)
}

fn has_url_userinfo(url: &Url) -> bool {
    if !url.username().is_empty() || url.password().is_some() {
        return true;
    }

    // `Url::username` cannot distinguish a missing username from an empty
    // userinfo component (`https://@example.test`). Inspect only the
    // authority portion so an `@` in the path does not count as userinfo.
    url.as_str()
        .split_once("://")
        .and_then(|(_, rest)| rest.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn is_loopback_host(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    host.trim_end_matches('.').eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}
