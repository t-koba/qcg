use crate::graph::Graph;
use camino::{Utf8Path, Utf8PathBuf};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read as _;

use super::validate::{Manifest, validate_asset_files, validate_resource_files};

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("failed to read manifest `{path}`: {source}")]
    Read {
        path: Utf8PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse manifest `{path}`: {source}")]
    Parse {
        path: Utf8PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid manifest: {0}")]
    Invalid(String),
    #[error("input `{field}` is too large: {actual_bytes} bytes exceeds {limit_bytes} bytes")]
    PayloadTooLarge {
        field: String,
        actual_bytes: usize,
        limit_bytes: usize,
    },
    #[error("invalid graph: {0}")]
    Graph(String),
}

#[derive(Debug, Clone)]
pub struct Contract {
    pub root: Utf8PathBuf,
    pub manifest: Manifest,
    pub graph: Graph,
    pub sha256: String,
}

impl Contract {
    pub fn load(root: impl AsRef<Utf8Path>) -> Result<Self, ContractError> {
        Self::load_with_limit(root, None)
    }

    /// Load a contract, enforcing an explicit manifest size limit only when set.
    /// `None` means no mechanistic limit; the caller sets a max only when desired.
    pub fn load_with_limit(
        root: impl AsRef<Utf8Path>,
        max_bytes: Option<usize>,
    ) -> Result<Self, ContractError> {
        let root = root.as_ref().to_path_buf();
        let manifest_path = root.join("qcg.toml");
        let source = read_manifest_with_limit(&manifest_path, max_bytes)?;
        let manifest: Manifest =
            toml::from_str(&source).map_err(|source| ContractError::Parse {
                path: manifest_path.clone(),
                source,
            })?;
        let graph = Graph::build(&manifest)
            .map_err(|error| ContractError::Graph(with_line_hint(&source, &error)))?;
        manifest
            .validate()
            .map_err(|error| error.with_line_hint(&source))?;
        validate_qcg_version(&manifest.generator.qcg_version, &manifest.generator.id)?;
        validate_asset_files(&root, &manifest.assets)?;
        validate_resource_files(&root, &manifest.resources)?;
        let sha256 = hex::encode(Sha256::digest(source.as_bytes()));
        Ok(Self {
            root,
            manifest,
            graph,
            sha256,
        })
    }

    pub fn line_hint(&self, message: &str) -> String {
        let source =
            read_manifest_with_limit(&self.root.join("qcg.toml"), None).unwrap_or_default();
        with_line_hint(&source, message)
    }

    /// Resolve an existing path declared by this generator package.
    pub fn resolve_package_path(
        &self,
        relative: &str,
    ) -> Result<Utf8PathBuf, crate::PackagePathError> {
        crate::resolve_package_path(&self.root, relative)
    }
}

pub(crate) fn read_manifest_with_limit(
    path: &Utf8Path,
    max_bytes: Option<usize>,
) -> Result<String, ContractError> {
    if let Some(max_bytes) = max_bytes {
        let file = fs::File::open(path).map_err(|source| ContractError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let limit = u64::try_from(max_bytes)
            .expect("manifest byte limit must fit in u64")
            .saturating_add(1);
        let mut source = String::new();
        file.take(limit)
            .read_to_string(&mut source)
            .map_err(|source| ContractError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        if source.len() > max_bytes {
            return Err(ContractError::Invalid(format!(
                "manifest `{path}` exceeds {max_bytes} bytes"
            )));
        }
        return Ok(source);
    }
    fs::read_to_string(path).map_err(|source| ContractError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_qcg_version(requirement: &str, generator_id: &str) -> Result<(), ContractError> {
    let requirement = requirement.trim();
    if requirement.is_empty() {
        return Err(ContractError::Invalid(format!(
            "generator `{generator_id}` must declare generator.qcg_version"
        )));
    }
    let requirement = semver::VersionReq::parse(requirement).map_err(|error| {
        ContractError::Invalid(format!(
            "generator.qcg_version `{requirement}` is invalid: {error}"
        ))
    })?;
    let current = semver::Version::parse(env!("CARGO_PKG_VERSION")).map_err(|error| {
        ContractError::Invalid(format!("qcg runtime version is invalid: {error}"))
    })?;
    if requirement.matches(&current) {
        Ok(())
    } else {
        Err(ContractError::Invalid(format!(
            "generator requires qcg_version `{requirement}`, runtime is `{}`",
            env!("CARGO_PKG_VERSION")
        )))
    }
}

impl ContractError {
    fn with_line_hint(self, source: &str) -> Self {
        match self {
            ContractError::Invalid(message) => {
                ContractError::Invalid(with_line_hint(source, &message))
            }
            ContractError::Graph(message) => ContractError::Graph(with_line_hint(source, &message)),
            other => other,
        }
    }
}

/// Drop the display prefix so aggregated messages do not repeat it per item.
pub(crate) fn stripped_error_message(error: ContractError) -> String {
    let message = error.to_string();
    message
        .strip_prefix("invalid manifest: ")
        .or_else(|| message.strip_prefix("invalid graph: "))
        .unwrap_or(&message)
        .to_string()
}

fn with_line_hint(source: &str, message: &str) -> String {
    let unknown_field = message
        .split_once("unknown field `")
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(field, _)| field);
    let needle = unknown_field.unwrap_or_else(|| {
        message
            .split('`')
            .enumerate()
            .filter(|(index, token)| index % 2 == 1 && !token.is_empty())
            .map(|(_, token)| token)
            .filter(|token| source.contains(token))
            .last()
            .unwrap_or_default()
    });
    if !needle.is_empty()
        && let Some(offset) = source.find(needle)
    {
        let line = source[..offset]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        return format!("line {line}: {message}");
    }
    format!("line 1: {message}")
}
