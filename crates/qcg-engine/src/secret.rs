use qcg_contract::SecretRef;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
#[cfg(unix)]
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

const MAX_SECRET_FILE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Default)]
pub struct SecretStore {
    values: BTreeMap<String, String>,
}

impl fmt::Debug for SecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretStore")
            .field("secret_count", &self.values.len())
            .finish()
    }
}

impl SecretStore {
    pub fn from_values(values: BTreeMap<String, String>) -> Self {
        Self {
            values: values
                .into_iter()
                .filter(|(_, value)| !value.is_empty())
                .collect(),
        }
    }

    pub fn try_from_env(secrets: &BTreeMap<String, SecretRef>) -> Result<Self, String> {
        let mut values = BTreeMap::new();
        for (name, secret) in secrets {
            let value = match (&secret.env, &secret.file_env) {
                (Some(env), None) => read_environment_value(env)?,
                (None, Some(file_env)) => match read_environment_value(file_env)? {
                    Some(path) => Some(read_secret_file(name, Path::new(&path))?),
                    None => None,
                },
                _ => {
                    return Err(format!(
                        "secret `{name}` must declare exactly one of env or file_env"
                    ));
                }
            };
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                values.insert(name.clone(), value);
            }
        }
        Ok(Self { values })
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    pub fn assert_absent(&self, text: &str) -> Result<(), String> {
        for (name, value) in &self.values {
            if !value.is_empty() && text.contains(value) {
                return Err(format!("secret `{name}` value was found in LLM context"));
            }
        }
        Ok(())
    }

    pub fn inject_declared_placeholders(
        &self,
        text: &str,
        allowed: &[String],
    ) -> Result<String, String> {
        let mut output = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(start) = rest.find("{{QCG_SECRET:") {
            output.push_str(&rest[..start]);
            let after_start = &rest[start + "{{QCG_SECRET:".len()..];
            let Some(end) = after_start.find("}}") else {
                return Err("unterminated QCG secret placeholder".into());
            };
            let name = after_start[..end].trim();
            if !allowed.iter().any(|allowed| allowed == name) {
                return Err(format!(
                    "secret `{name}` is used but is not declared by the transform"
                ));
            }
            let value = self
                .get(name)
                .ok_or_else(|| format!("secret `{name}` is not available"))?;
            output.push_str(value);
            rest = &after_start[end + "}}".len()..];
        }
        output.push_str(rest);
        Ok(output)
    }
}

fn read_environment_value(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "secret source environment variable `{name}` is not valid UTF-8"
        )),
    }
}

fn read_secret_file(secret_name: &str, path: &Path) -> Result<String, String> {
    if !path.is_absolute() {
        return Err(format!("secret `{secret_name}` file path must be absolute"));
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        format!(
            "secret `{secret_name}` file `{}` cannot be inspected: {error}",
            path.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "secret `{secret_name}` path `{}` must be a regular non-symlink file",
            path.display()
        ));
    }
    if metadata.len() > MAX_SECRET_FILE_BYTES {
        return Err(format!(
            "secret `{secret_name}` file exceeds {MAX_SECRET_FILE_BYTES} bytes"
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "secret `{secret_name}` file permissions must not grant group or other access"
            ));
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        read_open_secret_file(
            secret_name,
            path,
            options.open(path).map_err(|error| {
                format!(
                    "secret `{secret_name}` file `{}` cannot be opened safely: {error}",
                    path.display()
                )
            })?,
        )
    }
    #[cfg(not(unix))]
    {
        read_open_secret_file(
            secret_name,
            path,
            File::open(path).map_err(|error| {
                format!(
                    "secret `{secret_name}` file `{}` cannot be opened: {error}",
                    path.display()
                )
            })?,
        )
    }
}

fn read_open_secret_file(secret_name: &str, path: &Path, file: File) -> Result<String, String> {
    let metadata = file.metadata().map_err(|error| {
        format!(
            "secret `{secret_name}` file `{}` cannot be inspected after opening: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() || metadata.len() > MAX_SECRET_FILE_BYTES {
        return Err(format!(
            "secret `{secret_name}` file must be a regular file no larger than {MAX_SECRET_FILE_BYTES} bytes"
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_SECRET_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            format!(
                "secret `{secret_name}` file `{}` cannot be read: {error}",
                path.display()
            )
        })?;
    if bytes.len() as u64 > MAX_SECRET_FILE_BYTES {
        return Err(format!(
            "secret `{secret_name}` file exceeds {MAX_SECRET_FILE_BYTES} bytes"
        ));
    }
    let value = String::from_utf8(bytes)
        .map_err(|_| format!("secret `{secret_name}` file is not valid UTF-8"))?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value)
        .to_string();
    if value.is_empty() {
        return Err(format!("secret `{secret_name}` file is empty"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io::Write;

    #[test]
    fn injects_declared_secret_placeholders() {
        let mut store = SecretStore::default();
        store.values.insert("token".into(), "secret-value".into());
        let output = store
            .inject_declared_placeholders("x={{QCG_SECRET:token}}", &["token".into()])
            .unwrap();
        assert_eq!(output, "x=secret-value");
    }

    #[test]
    fn rejects_placeholder_not_declared_by_transform() {
        let store =
            SecretStore::from_values(BTreeMap::from([("token".into(), "secret-value".into())]));
        let error = store
            .inject_declared_placeholders("x={{QCG_SECRET:token}}", &[])
            .expect_err("undeclared placeholder must be rejected");
        assert!(error.contains("not declared"));
    }

    #[test]
    fn detects_secret_values_in_context() {
        let store =
            SecretStore::from_values(BTreeMap::from([("token".into(), "secret-value".into())]));
        assert!(store.assert_absent("leak secret-value").is_err());
    }

    #[test]
    fn debug_redacts_secret_values() {
        let store =
            SecretStore::from_values(BTreeMap::from([("token".into(), "secret-value".into())]));
        let debug = format!("{store:?}");
        assert!(debug.contains("secret_count"));
        assert!(!debug.contains("secret-value"));
        assert!(!debug.contains("token"));
    }

    #[cfg(unix)]
    #[test]
    fn reads_bounded_private_secret_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("token");
        let mut file = File::create(&path).expect("secret file should be created");
        file.write_all(b"secret-value\n")
            .expect("secret file should be written");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("secret permissions should be set");

        assert_eq!(
            read_secret_file("token", &path).expect("private secret file should be accepted"),
            "secret-value"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_secret_file_with_group_access() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("token");
        std::fs::write(&path, "secret-value").expect("secret file should be written");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
            .expect("secret permissions should be set");

        let error = read_secret_file("token", &path)
            .expect_err("group-readable secret file must be rejected");
        assert!(error.contains("group or other"));
    }
}
