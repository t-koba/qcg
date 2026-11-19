//! Deployable OIDC inputs for standalone subscription authentication.
//!
//! These values are policy: they select where an issuer lives and which
//! parameters are requested, not how OIDC validation works. All transport and
//! signature checks stay in the CLI auth adapter; the discovery endpoint and
//! every OAuth endpoint remain derived from the configured issuer and are
//! still constrained to its HTTPS origin.

use serde::Deserialize;
use url::Url;

/// Shipped issuer; also the origin anchor for every OIDC HTTP request.
pub const DEFAULT_ISSUER: &str = "https://auth.openai.com";
/// Shipped OAuth `resource` parameter.
pub const DEFAULT_RESOURCE: &str = "https://api.openai.com/v1";
/// Shipped client id used for first-time dynamic registration.
pub const DEFAULT_REGISTRATION_CLIENT_ID: &str = "dynamic_agent_client";
/// Shipped registration name hint (`agent_name_hint`).
pub const DEFAULT_CLIENT_ID_HINT: &str = "qcg";
/// Shipped scopes, in wire order (`openid` is added by the OIDC library).
pub const DEFAULT_SCOPES: &[&str] = &[
    "profile",
    "email",
    "offline_access",
    "resource.invoke",
    "chatgpt.tokens.use.direct",
];

const MAX_VALUE_BYTES: usize = 256;
const MAX_SCOPES: usize = 32;

/// `[subscription_auth]` section of the providers registry. Every field is
/// optional: an absent field keeps its shipped value, so today's wire requests
/// stay byte-identical when the section is absent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionAuthConfig {
    pub issuer: Option<String>,
    pub resource: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub registration_client_id: Option<String>,
    pub client_id_hint: Option<String>,
}

impl SubscriptionAuthConfig {
    pub fn issuer(&self) -> &str {
        self.issuer.as_deref().unwrap_or(DEFAULT_ISSUER)
    }

    pub fn resource(&self) -> &str {
        self.resource.as_deref().unwrap_or(DEFAULT_RESOURCE)
    }

    pub fn scopes(&self) -> Vec<String> {
        self.scopes
            .clone()
            .unwrap_or_else(|| DEFAULT_SCOPES.iter().map(|s| (*s).to_owned()).collect())
    }

    pub fn registration_client_id(&self) -> &str {
        self.registration_client_id
            .as_deref()
            .unwrap_or(DEFAULT_REGISTRATION_CLIENT_ID)
    }

    pub fn client_id_hint(&self) -> &str {
        self.client_id_hint
            .as_deref()
            .unwrap_or(DEFAULT_CLIENT_ID_HINT)
    }

    /// Refuses values that could turn an issuer-anchored OIDC flow into a
    /// resource-rooted or credential-leaking one. Endpoint URLs are not
    /// configurable: discovery finds them under the issuer's HTTPS origin.
    pub fn validate(&self) -> Result<(), String> {
        validate_https_url("issuer", self.issuer.as_deref())?;
        validate_https_url("resource", self.resource.as_deref())?;
        validate_value(
            "registration_client_id",
            self.registration_client_id.as_deref(),
        )?;
        validate_value("client_id_hint", self.client_id_hint.as_deref())?;
        if let Some(scopes) = &self.scopes {
            if scopes.is_empty() || scopes.len() > MAX_SCOPES {
                return Err(format!(
                    "subscription_auth scopes must contain 1..={MAX_SCOPES} entries"
                ));
            }
            for scope in scopes {
                validate_value("scope", Some(scope))?;
                if scope.chars().any(char::is_whitespace) {
                    return Err("subscription_auth scope must not contain whitespace".into());
                }
            }
        }
        Ok(())
    }
}

fn validate_https_url(field: &str, value: Option<&str>) -> Result<(), String> {
    let Some(value) = value else { return Ok(()) };
    validate_value(field, Some(value))?;
    let Ok(url) = Url::parse(value) else {
        return Err(format!("subscription_auth {field} must be a valid URL"));
    };
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(format!(
            "subscription_auth {field} must be an HTTPS URL without credentials, query or fragment"
        ));
    }
    Ok(())
}

fn validate_value(field: &str, value: Option<&str>) -> Result<(), String> {
    let Some(value) = value else { return Ok(()) };
    if value.is_empty() || value.len() > MAX_VALUE_BYTES || value.chars().any(char::is_control) {
        return Err(format!(
            "subscription_auth {field} must contain 1..={MAX_VALUE_BYTES} bytes without control characters"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_defaults_are_the_values_used_today() {
        let config = SubscriptionAuthConfig::default();
        assert_eq!(config.issuer(), "https://auth.openai.com");
        assert_eq!(config.resource(), "https://api.openai.com/v1");
        assert_eq!(config.registration_client_id(), "dynamic_agent_client");
        assert_eq!(config.client_id_hint(), "qcg");
        assert_eq!(
            config.scopes(),
            DEFAULT_SCOPES
                .iter()
                .map(|s| (*s).to_owned())
                .collect::<Vec<_>>()
        );
        config.validate().unwrap();
    }

    #[test]
    fn configured_values_are_read_and_unknown_keys_are_refused() {
        let config: SubscriptionAuthConfig = serde_json::from_str(
            r#"{"issuer":"https://issuer.example/tenant","resource":"https://api.example/v2",
                "scopes":["one","two"],"registration_client_id":"registration","client_id_hint":"hint"}"#,
        )
        .unwrap();
        assert_eq!(config.issuer(), "https://issuer.example/tenant");
        assert_eq!(config.resource(), "https://api.example/v2");
        assert_eq!(config.scopes(), vec!["one", "two"]);
        assert_eq!(config.registration_client_id(), "registration");
        assert_eq!(config.client_id_hint(), "hint");
        config.validate().unwrap();
        let error =
            serde_json::from_str::<SubscriptionAuthConfig>(r#"{"unknown":true}"#).unwrap_err();
        assert!(error.to_string().contains("unknown"), "{error}");
    }

    #[test]
    fn validation_keeps_https_issuer_anchor_and_bounded_values() {
        for text in [
            r#"{"issuer":"http://issuer.example"}"#,
            r#"{"issuer":"https://user@issuer.example"}"#,
            r#"{"issuer":"https://issuer.example/path?x=1"}"#,
            r#"{"resource":"http://api.example"}"#,
            r#"{"registration_client_id":""}"#,
            r#"{"client_id_hint":"bad\nname"}"#,
            r#"{"scopes":[]}"#,
            r#"{"scopes":["bad scope"]}"#,
        ] {
            let config: SubscriptionAuthConfig = serde_json::from_str(text).unwrap();
            assert!(config.validate().is_err(), "{text} must be refused");
        }
    }
}
