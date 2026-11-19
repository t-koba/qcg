//! Trusted configuration for the anonymous public MCP research profiles.
//!
//! Endpoints and bounds live in the shipped providers registry, not in
//! mechanism code. Deployments tune or disable them with the same table in
//! their own registry. This module only parses that table and enforces the
//! invariants that make a row safe to expose as a public profile.

use serde::Deserialize;
use std::sync::OnceLock;
use toml::Value;
use toml::map::Map;

use super::spec::McpServerSpec;
use super::transport::{McpAuth, McpTransport};
use super::validate::validate_remote_url;

const SHIPPED_REGISTRY: &str = include_str!("../../../../providers.toml");

#[derive(Deserialize)]
struct PublicProfileSection {
    #[serde(default)]
    public_mcp_server: Option<Value>,
}

/// Raw shipped rows, parsed once from the registry embedded at build time.
fn shipped_public_tables() -> &'static Map<String, Value> {
    static SHIPPED: OnceLock<Map<String, Value>> = OnceLock::new();
    SHIPPED.get_or_init(|| {
        let section: PublicProfileSection =
            toml::from_str(SHIPPED_REGISTRY).expect("shipped registry must be valid TOML");
        section
            .public_mcp_server
            .as_ref()
            .and_then(Value::as_table)
            .expect("shipped registry must declare a public MCP profile table")
            .clone()
    })
}

/// Ids reserved for shipped public research profiles. The shipped registry
/// table is their single source: regular `mcp_server` rows may never claim
/// them, and changing the shipped rows changes this namespace automatically.
fn reserved_public_ids() -> &'static [&'static str] {
    static IDS: std::sync::OnceLock<Vec<&'static str>> = std::sync::OnceLock::new();
    IDS.get_or_init(|| {
        shipped_public_tables()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
    })
    .as_slice()
}

/// Shipped defaults, parsed once from the registry embedded at build time.
/// This is the fallback for runs without a registry and the base every
/// configured override inherits.
pub fn shipped_public_profiles() -> &'static [McpServerSpec] {
    static PROFILES: OnceLock<Vec<McpServerSpec>> = OnceLock::new();
    PROFILES.get_or_init(|| {
        let specs: Vec<McpServerSpec> = shipped_public_tables()
            .iter()
            .map(|(id, entry)| {
                let spec: McpServerSpec = entry
                    .clone()
                    .try_into()
                    .expect("shipped public MCP profile must be a valid spec");
                assert_eq!(&spec.id, id, "shipped profile key must match its id");
                validate_public_spec(&spec).expect("shipped public MCP profile must be safe");
                spec
            })
            .collect();
        assert_eq!(specs.len(), reserved_public_ids().len());
        specs
    })
}

/// A trusted public table resolved against the shipped rows. Unspecified
/// fields keep their shipped values; `enabled = false` removes one profile.
#[derive(Debug, Clone)]
pub struct PublicProfiles {
    specs: Vec<McpServerSpec>,
}

impl PublicProfiles {
    /// Resolves a registry's `public_mcp_server` table. Every reserved id not
    /// named keeps its shipped row; `enabled = false` removes one profile (or
    /// all when set on the table). Unknown keys are refused by serde and every
    /// resulting row is validated before it can reach the runtime.
    pub fn from_value(value: &Value) -> Result<Self, String> {
        let overrides = value
            .as_table()
            .ok_or_else(|| "`public_mcp_server` must be a table".to_owned())?;
        let mut specs = shipped_public_profiles().to_vec();
        let all_enabled = match overrides.get("enabled") {
            Some(value) => value
                .as_bool()
                .ok_or_else(|| "`public_mcp_server` enabled must be a boolean".to_owned())?,
            None => true,
        };
        for (id, entry) in overrides {
            if id == "enabled" {
                continue;
            }
            if !reserved_public_ids().contains(&id.as_str()) {
                return Err(format!(
                    "public MCP profile id `{id}` is not reserved; expected one of {}",
                    reserved_public_ids().join(", ")
                ));
            }
            let fields = entry
                .as_table()
                .ok_or_else(|| format!("public MCP profile `{id}` must be a table"))?;
            let shipped = shipped_public_tables()
                .get(id)
                .and_then(Value::as_table)
                .expect("reserved public ids come from the shipped table");
            let mut merged = Value::Table(shipped.clone());
            let merged_table = merged
                .as_table_mut()
                .expect("a shipped public row is a table");
            let mut enabled = true;
            for (key, value) in fields {
                if key == "enabled" {
                    enabled = value.as_bool().ok_or_else(|| {
                        format!("public MCP profile `{id}` enabled must be a boolean")
                    })?;
                } else {
                    merged_table.insert(key.clone(), value.clone());
                }
            }
            let position = specs
                .iter()
                .position(|spec| spec.id == *id)
                .expect("reserved public ids ship a row");
            let spec: McpServerSpec = merged
                .try_into()
                .map_err(|error| format!("invalid public MCP profile `{id}`: {error}"))?;
            if spec.id != *id {
                return Err(format!(
                    "public MCP profile `{id}` must not override its id"
                ));
            }
            validate_public_spec(&spec)?;
            if enabled {
                specs[position] = spec;
            } else {
                specs.remove(position);
            }
        }
        // Validate every row even when the whole table is disabled: bad keys
        // must fail closed regardless of whether the profile would register.
        if !all_enabled {
            specs.clear();
        }
        Ok(Self { specs })
    }

    pub fn specs(&self) -> &[McpServerSpec] {
        &self.specs
    }

    pub fn into_specs(self) -> Vec<McpServerSpec> {
        self.specs
    }
}

/// Parses the public table of a providers registry text. `None` means the
/// registry did not declare the table and the shipped defaults apply.
pub fn parse_public_profiles(text: &str) -> Result<Option<PublicProfiles>, String> {
    let section: PublicProfileSection = toml::from_str(text)
        .map_err(|error| format!("invalid public MCP profile configuration: {error}"))?;
    section
        .public_mcp_server
        .as_ref()
        .map(PublicProfiles::from_value)
        .transpose()
}

pub fn is_reserved_public_id(id: &str) -> bool {
    reserved_public_ids().contains(&id)
}

/// Fail-closed checks for configured public profiles: anonymous, HTTPS,
/// pinned to the endpoint host, and free of commands, environment and
/// credential material. Contract-level tool bindings and host permissions
/// still gate every call.
pub fn validate_public_spec(spec: &McpServerSpec) -> Result<(), String> {
    if spec.allow_plaintext_private_http {
        return Err("public MCP profiles must not enable allow_plaintext_private_http".into());
    }
    spec.validate()?;
    if !is_reserved_public_id(&spec.id) {
        return Err(format!(
            "public MCP profile id `{}` is not reserved; expected one of {}",
            spec.id,
            reserved_public_ids().join(", ")
        ));
    }
    if spec.transport != McpTransport::StreamableHttp {
        return Err(format!(
            "public MCP profile `{}` must use streamable_http",
            spec.id
        ));
    }
    if spec.auth != McpAuth::None || spec.credential_env.is_some() {
        return Err(format!(
            "public MCP profile `{}` must be anonymous and declare no credential",
            spec.id
        ));
    }
    if !spec.headers.is_empty() {
        return Err(format!(
            "public MCP profile `{}` must not declare static headers",
            spec.id
        ));
    }
    let raw = spec
        .url
        .as_deref()
        .expect("HTTP spec validation requires a url");
    let url = validate_remote_url(&spec.id, raw)?;
    if url.scheme() != "https" {
        return Err(format!(
            "public MCP profile `{}` url must use HTTPS",
            spec.id
        ));
    }
    let host = url.host_str().expect("URL validation requires a host");
    if spec.allowed_hosts.len() != 1 || spec.allowed_hosts[0] != host {
        return Err(format!(
            "public MCP profile `{}` allowed_hosts must be exactly [`{host}`]",
            spec.id
        ));
    }
    Ok(())
}
