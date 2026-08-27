use qcg_contract::SecretRef;
use std::collections::BTreeMap;
use std::fmt;

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

    pub fn from_env(secrets: &BTreeMap<String, SecretRef>) -> Self {
        let values = secrets
            .iter()
            .filter_map(|(name, secret)| {
                std::env::var(&secret.env)
                    .ok()
                    .map(|value| (name.clone(), value))
            })
            .filter(|(_, value)| !value.is_empty())
            .collect();
        Self { values }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
