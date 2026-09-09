use camino::{Utf8Path, Utf8PathBuf};
use qcg_fs::read_bounded;
use qcg_policy::{is_safe_relative_path, portable_relative_path};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read as _;

use crate::types::ServiceError;

/// Explicit max only. `None` means no mechanistic limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct PackageLimits {
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub max_metadata_bytes: Option<usize>,
    pub max_archive_bytes: Option<u64>,
}

/// Explicit max only. `None` means no mechanistic limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArtifactZipLimits {
    pub max_bytes: Option<u64>,
    pub max_entries: Option<usize>,
}

pub fn unpack_qcg(
    archive: &Utf8Path,
    target: &Utf8Path,
    limits: &PackageLimits,
) -> Result<(), ServiceError> {
    let file = File::open(archive).map_err(ServiceError::Io)?;
    let mut archive = zip::ZipArchive::new(file)?;
    if limits
        .max_entries
        .is_some_and(|limit| archive.len() > limit)
    {
        return Err(ServiceError::Invalid(format!(
            "archive contains too many entries: {} > {}",
            archive.len(),
            limits.max_entries.unwrap_or(usize::MAX)
        )));
    }
    let mut unpacked_bytes = 0_u64;
    let mut paths = BTreeSet::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let Some(enclosed) = entry.enclosed_name() else {
            return Err(ServiceError::Invalid(format!(
                "archive contains an unsafe path: {}",
                entry.name()
            )));
        };
        let rel = Utf8PathBuf::from_path_buf(enclosed.to_path_buf())
            .map_err(|_| ServiceError::Invalid("archive path is not UTF-8".into()))?;
        if rel.as_str().is_empty() {
            continue;
        }
        if !paths.insert(rel.clone()) {
            return Err(ServiceError::Invalid(format!(
                "archive contains duplicate path `{rel}`"
            )));
        }
        if entry
            .unix_mode()
            .is_some_and(|mode| mode & 0o170000 == 0o120000)
        {
            return Err(ServiceError::Invalid(format!(
                "archive contains a symbolic link `{rel}`"
            )));
        }
        unpacked_bytes = unpacked_bytes
            .checked_add(entry.size())
            .ok_or_else(|| ServiceError::Invalid("archive expanded size overflowed".into()))?;
        if limits.max_bytes.is_some_and(|limit| unpacked_bytes > limit) {
            return Err(ServiceError::Invalid(format!(
                "archive expanded size exceeds {} bytes",
                limits.max_bytes.unwrap_or(u64::MAX)
            )));
        }
        let out = target.join(&rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&out)?;
        } else {
            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut output = File::create(&out)?;
            let copied = std::io::copy(&mut entry, &mut output)?;
            if copied != entry.size() {
                return Err(ServiceError::Invalid(format!(
                    "archive entry size changed while unpacking `{rel}`"
                )));
            }
        }
    }
    verify_package_inventory(target, limits)?;
    Ok(())
}

pub fn copy_dir_all(
    source: &Utf8Path,
    target: &Utf8Path,
    limits: &PackageLimits,
) -> Result<(), ServiceError> {
    let mut entry_count = 0_usize;
    let mut copied_bytes = 0_u64;
    for entry in qcg_fs::WalkDir::new(source) {
        let entry = entry.map_err(|error| {
            ServiceError::Invalid(format!("failed to walk `{source}`: {error}"))
        })?;
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            return Err(ServiceError::Invalid(format!(
                "package source contains a symbolic link: {}",
                entry.path()
            )));
        }
        let path = entry.path().to_path_buf();
        let rel = path
            .strip_prefix(source)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        if rel.as_str().is_empty() {
            if !file_type.is_dir() {
                return Err(ServiceError::Invalid(format!(
                    "package source is not a directory: {path}"
                )));
            }
            continue;
        }
        entry_count = entry_count
            .checked_add(1)
            .ok_or_else(|| ServiceError::Invalid("package entry count overflowed".into()))?;
        if limits.max_entries.is_some_and(|limit| entry_count > limit) {
            return Err(ServiceError::Invalid(format!(
                "package source contains too many entries: {entry_count} > {}",
                limits.max_entries.unwrap_or(usize::MAX)
            )));
        }
        if !is_safe_relative_path(&portable_relative_path(rel)) {
            return Err(ServiceError::Invalid(format!(
                "package source contains an unsafe path `{rel}`"
            )));
        }
        let dest = target.join(rel);
        if file_type.is_dir() {
            std::fs::create_dir_all(&dest)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(ServiceError::Invalid(format!(
                "package source contains an unsupported entry: {path}"
            )));
        }
        let metadata = entry.metadata().map_err(|error| {
            ServiceError::Invalid(format!("failed to inspect package entry: {error}"))
        })?;
        copied_bytes = copied_bytes
            .checked_add(metadata.len())
            .ok_or_else(|| ServiceError::Invalid("package source size overflowed".into()))?;
        if limits.max_bytes.is_some_and(|limit| copied_bytes > limit) {
            return Err(ServiceError::Invalid(format!(
                "package source exceeds {} bytes",
                limits.max_bytes.unwrap_or(u64::MAX)
            )));
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let copied = std::fs::copy(&path, &dest)?;
        if copied != metadata.len() {
            return Err(ServiceError::Invalid(format!(
                "package source changed while being copied: {path}"
            )));
        }
    }
    Ok(())
}

fn verify_package_inventory(root: &Utf8Path, limits: &PackageLimits) -> Result<(), ServiceError> {
    let sbom_path = root.join("QCG-SBOM.spdx.json");
    let provenance_path = root.join("QCG-PROVENANCE.intoto.json");
    let sbom_bytes = read_bounded(&sbom_path, limits.max_metadata_bytes).map_err(|error| {
        ServiceError::Invalid(format!("package is missing QCG-SBOM.spdx.json: {error}"))
    })?;
    let provenance_bytes =
        read_bounded(&provenance_path, limits.max_metadata_bytes).map_err(|error| {
            ServiceError::Invalid(format!(
                "package is missing QCG-PROVENANCE.intoto.json: {error}"
            ))
        })?;
    let sbom: serde_json::Value =
        serde_json::from_slice(&sbom_bytes).map_err(ServiceError::Json)?;
    let provenance: serde_json::Value =
        serde_json::from_slice(&provenance_bytes).map_err(ServiceError::Json)?;
    if sbom.get("spdxVersion").and_then(|value| value.as_str()) != Some("SPDX-2.3")
        || provenance.get("_type").and_then(|value| value.as_str())
            != Some("https://in-toto.io/Statement/v1")
    {
        return Err(ServiceError::Invalid(
            "package supply-chain metadata has an unsupported format".into(),
        ));
    }
    let mut expected = BTreeMap::new();
    let files = sbom
        .get("files")
        .and_then(|value| value.as_array())
        .ok_or_else(|| ServiceError::Invalid("package SBOM files array is required".into()))?;
    for file in files {
        let path = file
            .get("fileName")
            .and_then(|value| value.as_str())
            .ok_or_else(|| ServiceError::Invalid("package SBOM fileName is required".into()))?;
        if !is_safe_relative_path(path) {
            return Err(ServiceError::Invalid(format!(
                "package SBOM contains unsafe path `{path}`"
            )));
        }
        let sha256 = file
            .pointer("/checksums/0/checksumValue")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                ServiceError::Invalid("package SBOM SHA256 checksum is required".into())
            })?;
        if expected
            .insert(path.to_string(), sha256.to_string())
            .is_some()
        {
            return Err(ServiceError::Invalid(format!(
                "package SBOM contains duplicate path `{path}`"
            )));
        }
    }
    let mut actual = BTreeSet::new();
    for entry in qcg_fs::WalkDir::new(root) {
        let entry = entry
            .map_err(|error| ServiceError::Invalid(format!("failed to walk package: {error}")))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path().to_path_buf();
        let relative = portable_relative_path(
            path.strip_prefix(root)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        );
        if matches!(
            relative.as_str(),
            "QCG-SBOM.spdx.json" | "QCG-PROVENANCE.intoto.json"
        ) {
            continue;
        }
        actual.insert(relative.clone());
        let expected_sha256 = expected.get(&relative).ok_or_else(|| {
            ServiceError::Invalid(format!("package contains unlisted file `{relative}`"))
        })?;
        let digest = sha256_file(&path)?;
        if &digest != expected_sha256 {
            return Err(ServiceError::Invalid(format!(
                "package file `{relative}` failed SBOM integrity verification"
            )));
        }
    }
    for missing in expected.keys() {
        if !actual.contains(missing) {
            return Err(ServiceError::Invalid(format!(
                "package SBOM lists missing file `{missing}`"
            )));
        }
    }
    Ok(())
}

fn sha256_file(path: &Utf8Path) -> Result<String, ServiceError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read: usize = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}
