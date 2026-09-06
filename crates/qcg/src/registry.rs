//! Loosely-coupled generator registries.
//!
//! A registry is a static TOML index file served over `file://` or `https://`.
//! There is no registry server to run and no root privilege to install:
//! resolution happens client-side and everything lives under `$QCG_HOME`
//! (default `~/.qcg`).

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Home directory for registries and trusted keys. Everything qcg stores
/// here is ordinary user-owned files; no privilege is required.
pub fn home_dir() -> Result<Utf8PathBuf> {
    if let Ok(home) = std::env::var("QCG_HOME")
        && !home.trim().is_empty()
    {
        return Ok(Utf8PathBuf::from(home));
    }
    let user_home = std::env::var("HOME")
        .map_err(|_| anyhow::anyhow!("set QCG_HOME or HOME to locate the qcg home directory"))?;
    if user_home.trim().is_empty() {
        anyhow::bail!("set QCG_HOME or HOME to locate the qcg home directory");
    }
    Ok(Utf8PathBuf::from(user_home).join(".qcg"))
}

fn registries_path(home: &Utf8Path) -> Utf8PathBuf {
    home.join("registries.toml")
}

fn trusted_keys_path(home: &Utf8Path) -> Utf8PathBuf {
    home.join("trusted-keys.toml")
}

/// Named registry index URLs in configuration order.
#[derive(Debug, Default)]
pub struct RegistryConfig {
    pub registries: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RegistriesFile {
    #[serde(default)]
    registries: BTreeMap<String, String>,
}

pub fn load_registries(home: &Utf8Path) -> Result<RegistryConfig> {
    let path = registries_path(home);
    if !path.exists() {
        return Ok(RegistryConfig::default());
    }
    let source = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read registries `{path}`"))?;
    let file: RegistriesFile =
        toml::from_str(&source).with_context(|| format!("failed to parse registries `{path}`"))?;
    Ok(RegistryConfig {
        registries: file.registries,
    })
}

pub fn save_registries(home: &Utf8Path, config: &RegistryConfig) -> Result<()> {
    std::fs::create_dir_all(home).with_context(|| format!("failed to create qcg home `{home}`"))?;
    let mut source =
        String::from("# qcg registries: name to index URL (file:// or https://)\n[registries]\n");
    for (name, url) in &config.registries {
        source.push_str(&format!("{name} = \"{url}\"\n"));
    }
    std::fs::write(registries_path(home), source)
        .with_context(|| format!("failed to write registries under `{home}`"))?;
    Ok(())
}

/// A locally trusted signing key.
#[derive(Debug, Clone)]
pub struct TrustedKey {
    pub id: String,
    pub public_key: Vec<u8>,
    pub expires: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Deserialize)]
struct TrustedKeysFile {
    #[serde(default)]
    keys: Vec<TrustedKeyEntry>,
}

#[derive(Debug, Deserialize)]
struct TrustedKeyEntry {
    id: String,
    public_key: String,
    expires: Option<String>,
}

pub fn load_trusted_keys(home: &Utf8Path) -> Result<Vec<TrustedKey>> {
    let path = trusted_keys_path(home);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let source = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read trusted keys `{path}`"))?;
    let file: TrustedKeysFile = toml::from_str(&source)
        .with_context(|| format!("failed to parse trusted keys `{path}`"))?;
    let mut keys = Vec::new();
    for entry in file.keys {
        let public_key = hex::decode(entry.public_key.trim())
            .with_context(|| format!("trusted key `{}` public key must be hex", entry.id))?;
        if public_key.len() != 32 {
            anyhow::bail!(
                "trusted key `{}` public key must be 32 bytes, got {}",
                entry.id,
                public_key.len()
            );
        }
        let expires = entry
            .expires
            .map(|at| {
                at.parse::<chrono::DateTime<chrono::Utc>>()
                    .with_context(|| format!("trusted key `{}` expiry must be RFC 3339", entry.id))
            })
            .transpose()?;
        keys.push(TrustedKey {
            id: entry.id,
            public_key,
            expires,
        });
    }
    Ok(keys)
}

/// One row of a registry index file.
#[derive(Debug, Clone, Deserialize)]
pub struct IndexPackage {
    pub id: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    pub url: String,
    pub sha256: String,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub key_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IndexFile {
    #[serde(default)]
    packages: Vec<IndexPackage>,
}

impl IndexFile {
    pub fn packages_for_search(&self) -> &[IndexPackage] {
        &self.packages
    }
}

/// A resolved install candidate with its source registry.
#[derive(Debug, Clone)]
pub struct ResolvedPackage {
    pub entry: IndexPackage,
    pub registry: String,
}

/// Split a registry-style source (`id` or `id@version-requirement`).
/// Returns `None` for paths, URLs, and archives, which keep the existing
/// direct-install behavior.
pub fn split_id_source(source: &str) -> Option<(String, String)> {
    if source.contains('/')
        || source.contains('\\')
        || source.contains("://")
        || source.ends_with(".qcg")
        || source.ends_with(".zip")
        || source.ends_with(".tar.gz")
    {
        return None;
    }
    let (id, requirement) = match source.split_once('@') {
        Some((id, requirement)) => (id, requirement),
        None => (source, "*"),
    };
    if id.trim().is_empty() || id == "." || id == ".." {
        return None;
    }
    Some((id.to_string(), requirement.to_string()))
}
/// Fetch a registry index over `file://` or `https://`. Plain `http://` is
/// rejected: use a local file or TLS.
pub async fn fetch_index(url: &str) -> Result<IndexFile> {
    if let Some(path) = url.strip_prefix("file://") {
        let source = std::fs::read(path)
            .with_context(|| format!("failed to read registry index `{url}`"))?;
        return parse_index(&source, url);
    }
    if let Some(rest) = url.strip_prefix("https://") {
        let _ = rest;
        let response = reqwest::get(url)
            .await
            .with_context(|| format!("failed to fetch registry index `{url}`"))?
            .error_for_status()
            .with_context(|| format!("registry index `{url}` returned an error status"))?;
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("failed to read registry index `{url}`"))?;
        return parse_index(&bytes, url);
    }
    anyhow::bail!("registry index `{url}` must use file:// or https://");
}

fn parse_index(bytes: &[u8], url: &str) -> Result<IndexFile> {
    let source = std::str::from_utf8(bytes)
        .with_context(|| format!("registry index `{url}` must be UTF-8 TOML"))?;
    toml::from_str(source).with_context(|| format!("failed to parse registry index `{url}`"))
}

/// Canonical signature payload for an index row.
pub fn signature_payload(entry: &IndexPackage) -> String {
    format!(
        "{}\n{}\n{}\n{}\n",
        entry.id, entry.version, entry.url, entry.sha256
    )
}

/// Verify a signed index row against locally trusted keys. Unsigned rows
/// pass: transport integrity plus the entry sha256 still applies at install.
pub fn verify_package(entry: &IndexPackage, keys: &[TrustedKey]) -> Result<()> {
    let (signature, key_id) = match (&entry.signature, &entry.key_id) {
        (Some(signature), Some(key_id)) => (signature, key_id),
        (None, None) => return Ok(()),
        _ => anyhow::bail!(
            "package `{}@{}` must declare signature and key_id together",
            entry.id,
            entry.version
        ),
    };
    let key = keys.iter().find(|key| &key.id == key_id).ok_or_else(|| {
        anyhow::anyhow!(
            "package `{}@{}` is signed by untrusted key `{key_id}`",
            entry.id,
            entry.version
        )
    })?;
    if key
        .expires
        .is_some_and(|expires| expires <= chrono::Utc::now())
    {
        anyhow::bail!("trusted key `{key_id}` has expired");
    }
    let signature = hex::decode(signature.trim()).with_context(|| {
        format!(
            "package `{}@{}` signature must be hex",
            entry.id, entry.version
        )
    })?;
    aws_lc_rs::signature::UnparsedPublicKey::new(&aws_lc_rs::signature::ED25519, &key.public_key)
        .verify(signature_payload(entry).as_bytes(), &signature)
        .map_err(|_| {
            anyhow::anyhow!(
                "package `{}@{}` signature does not verify with key `{key_id}`",
                entry.id,
                entry.version
            )
        })?;
    Ok(())
}

/// Resolve the highest version matching `requirement` across registries in
/// configuration order. Ties keep the earliest registry.
pub async fn resolve(
    config: &RegistryConfig,
    id: &str,
    requirement: &semver::VersionReq,
) -> Result<ResolvedPackage> {
    if config.registries.is_empty() {
        anyhow::bail!("no registries configured; add one with `qcg registry add <name> <url>`");
    }
    let mut best: Option<(semver::Version, ResolvedPackage)> = None;
    for (registry, url) in &config.registries {
        let index = fetch_index(url).await?;
        for entry in index.packages {
            if entry.id != id {
                continue;
            }
            let version = semver::Version::parse(&entry.version).with_context(|| {
                format!(
                    "registry `{registry}` package `{id}` has an invalid version `{}`",
                    entry.version
                )
            })?;
            if !requirement.matches(&version) {
                continue;
            }
            let replace = best.as_ref().is_none_or(|(current, _)| version > *current);
            if replace {
                best = Some((
                    version,
                    ResolvedPackage {
                        entry,
                        registry: registry.clone(),
                    },
                ));
            }
        }
    }
    best.map(|(_, resolved)| resolved).ok_or_else(|| {
        anyhow::anyhow!("package `{id}` has no version matching `{requirement}` in any registry")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lc_rs::rand::SystemRandom;
    use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};

    fn test_keypair() -> Ed25519KeyPair {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .expect("test keypair should generate");
        Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("test keypair should parse")
    }

    fn signed_entry(key_id: &str, keypair: &Ed25519KeyPair) -> IndexPackage {
        let mut entry = IndexPackage {
            id: "demo".into(),
            version: "1.2.0".into(),
            description: String::new(),
            url: "file:///tmp/demo.qcg".into(),
            sha256: "0".repeat(64),
            signature: None,
            key_id: None,
        };
        let signature = keypair.sign(signature_payload(&entry).as_bytes());
        entry.signature = Some(hex::encode(signature.as_ref()));
        entry.key_id = Some(key_id.into());
        entry
    }

    fn trusted_key(id: &str, keypair: &Ed25519KeyPair) -> TrustedKey {
        TrustedKey {
            id: id.into(),
            public_key: keypair.public_key().as_ref().to_vec(),
            expires: None,
        }
    }

    #[test]
    fn split_id_source_separates_ids_from_paths() {
        assert_eq!(
            split_id_source("demo@^1.0").unwrap(),
            ("demo".to_string(), "^1.0".to_string())
        );
        assert_eq!(
            split_id_source("demo").unwrap(),
            ("demo".to_string(), "*".to_string())
        );
        assert!(split_id_source("./local").is_none());
        assert!(split_id_source("https://example.invalid/x.qcg").is_none());
        assert!(split_id_source("bundle.qcg").is_none());
    }

    #[test]
    fn package_signatures_verify_against_trusted_keys() {
        let keypair = test_keypair();
        let entry = signed_entry("main", &keypair);
        verify_package(&entry, &[trusted_key("main", &keypair)])
            .expect("valid signature should verify");

        let other = test_keypair();
        let error = verify_package(&entry, &[trusted_key("main", &other)])
            .expect_err("wrong key must fail");
        assert!(error.to_string().contains("does not verify"));

        let error = verify_package(&entry, &[]).expect_err("untrusted key must fail");
        assert!(error.to_string().contains("untrusted key"));

        let mut tampered = entry.clone();
        tampered.version = "9.9.9".into();
        let error = verify_package(&tampered, &[trusted_key("main", &keypair)])
            .expect_err("tampered version must fail");
        assert!(error.to_string().contains("does not verify"));

        let mut expired_key = trusted_key("main", &keypair);
        expired_key.expires = Some(chrono::Utc::now() - chrono::Duration::hours(1));
        let error = verify_package(&entry, &[expired_key]).expect_err("expired key must fail");
        assert!(error.to_string().contains("expired"));

        let unsigned = IndexPackage {
            signature: None,
            key_id: None,
            ..entry
        };
        verify_package(&unsigned, &[]).expect("unsigned rows pass transport trust");
    }

    #[test]
    fn index_urls_reject_plain_http() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let error = runtime
            .block_on(fetch_index("http://example.invalid/index.toml"))
            .expect_err("plain http must fail");
        assert!(error.to_string().contains("file:// or https://"));
    }
}
