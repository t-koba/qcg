use anyhow::{Context, Result};
use aws_lc_rs::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::StreamExt;
use qcg_contract::Contract;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use uuid::Uuid;

use super::plan::{confirm_stdin, ensure_safe_install_id, print_permission_summary};
use qcg_policy::MAX_SIGNING_KEY_BYTES;
use qcg_service::app_registry_with_providers as app_registry;

pub(crate) struct InstallVerification<'a> {
    pub(crate) sha256: Option<&'a str>,
    pub(crate) signature: Option<&'a str>,
    pub(crate) public_key: Option<&'a str>,
}

#[derive(Default)]
struct CleanupPaths {
    paths: Vec<Utf8PathBuf>,
}

impl CleanupPaths {
    fn push(&mut self, path: Utf8PathBuf) {
        self.paths.push(path);
    }
}

impl Drop for CleanupPaths {
    fn drop(&mut self) {
        for path in self.paths.drain(..) {
            let _ = remove_owned_path(&path);
        }
    }
}

struct StagedInstall {
    path: Utf8PathBuf,
    _cleanup: CleanupPaths,
}

pub(crate) struct CleanupPath {
    path: Option<Utf8PathBuf>,
}

impl CleanupPath {
    pub(crate) fn new(path: Utf8PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn cleanup(&mut self) -> Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        remove_owned_path(path)?;
        self.path = None;
        Ok(())
    }

    fn disarm(&mut self) {
        self.path = None;
    }

    fn path(&self) -> Option<&Utf8Path> {
        self.path.as_deref()
    }
}

impl Drop for CleanupPath {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = remove_owned_path(&path);
        }
    }
}

fn remove_owned_path(path: &Utf8Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

pub(crate) async fn install(
    providers_path: Option<&Utf8Path>,
    source: &str,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
    verification: InstallVerification<'_>,
    limits: &qcg_service::PackageLimits,
) -> Result<String> {
    if let Some((id, requirement)) = crate::registry::split_id_source(source)
        && !Utf8Path::new(source).exists()
    {
        let requirement = semver::VersionReq::parse(&requirement)
            .with_context(|| format!("version requirement `{requirement}` is invalid"))?;
        return install_registry_package(
            providers_path,
            &id,
            &requirement,
            generators_dir,
            yes,
            force,
            limits,
            &mut BTreeSet::new(),
        )
        .await;
    }
    let remote = source.starts_with("http://") || source.starts_with("https://");
    if verification.signature.is_some() != verification.public_key.is_some() {
        anyhow::bail!("--signature and --public-key must be supplied together");
    }
    if remote && verification.sha256.is_none() && verification.signature.is_none() {
        anyhow::bail!(
            "remote installs require --sha256 or an Ed25519 --signature with --public-key"
        );
    }
    let staged = stage_install_source(
        source,
        verification.sha256,
        verification.signature.zip(verification.public_key),
        limits,
    )
    .await?;
    finish_install(providers_path, staged, generators_dir, yes, force, limits)
}

/// Install a registry-resolved package plus its dependencies.
#[allow(clippy::too_many_arguments)]
fn install_registry_package<'a>(
    providers_path: Option<&'a Utf8Path>,
    id: &'a str,
    requirement: &'a semver::VersionReq,
    generators_dir: &'a Utf8Path,
    yes: bool,
    force: bool,
    limits: &'a qcg_service::PackageLimits,
    visiting: &'a mut BTreeSet<String>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + 'a>> {
    Box::pin(async move {
        if !visiting.insert(id.to_string()) {
            anyhow::bail!("dependency cycle detected at generator `{id}`");
        }
        if let Some(installed) = installed_generator_version(generators_dir, id)? {
            if requirement.matches(&installed) {
                println!("generator `{id}@{installed}` already satisfies `{requirement}`");
                return Ok(id.to_string());
            }
            if !force {
                anyhow::bail!(
                    "generator `{id}@{installed}` is installed but does not satisfy `{requirement}`; pass --force to replace it"
                );
            }
        }
        let home = crate::registry::home_dir()?;
        let config = crate::registry::load_registries(&home)?;
        let keys = crate::registry::load_trusted_keys(&home)?;
        let resolved = crate::registry::resolve(&config, id, requirement).await?;
        println!(
            "resolved `{}` to version {} from registry `{}`",
            id, resolved.entry.version, resolved.registry
        );
        crate::registry::verify_package(&resolved.entry, &keys)?;
        let public_key = resolved.entry.key_id.as_ref().and_then(|key_id| {
            keys.iter()
                .find(|key| &key.id == key_id)
                .map(|key| hex::encode(&key.public_key))
        });
        let staged = stage_install_source(
            &resolved.entry.url,
            Some(&resolved.entry.sha256),
            resolved
                .entry
                .signature
                .as_deref()
                .zip(public_key.as_deref()),
            limits,
        )
        .await?;
        let installed_id =
            finish_install(providers_path, staged, generators_dir, yes, true, limits)?;
        let manifest = Contract::load(generators_dir.join(&installed_id))?.manifest;
        for (dependency, requirement) in &manifest.dependencies {
            let requirement = semver::VersionReq::parse(requirement).with_context(|| {
                format!("dependency `{dependency}` version requirement is invalid")
            })?;
            install_registry_package(
                providers_path,
                dependency,
                &requirement,
                generators_dir,
                yes,
                force,
                limits,
                visiting,
            )
            .await?;
        }
        Ok(installed_id)
    })
}

fn installed_generator_version(
    generators_dir: &Utf8Path,
    id: &str,
) -> Result<Option<semver::Version>> {
    let manifest_path = generators_dir.join(id).join("qcg.toml");
    if !manifest_path.exists() {
        return Ok(None);
    }
    let contract = Contract::load(generators_dir.join(id))?;
    semver::Version::parse(&contract.manifest.generator.version)
        .map(Some)
        .with_context(|| format!("installed generator `{id}` has an invalid version"))
}
fn finish_install(
    providers_path: Option<&Utf8Path>,
    staged: StagedInstall,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
    limits: &qcg_service::PackageLimits,
) -> Result<String> {
    let contract = Contract::load(&staged.path)?;
    app_registry(providers_path)?.validate_contract(&contract)?;
    print_permission_summary(&contract);
    if !yes {
        confirm_stdin("Install this generator?")?;
    }
    let id = contract.manifest.generator.id.clone();
    ensure_safe_install_id(&id)?;
    std::fs::create_dir_all(generators_dir)?;
    let target = generators_dir.join(&id);
    let target_exists = match std::fs::symlink_metadata(&target) {
        Ok(_) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect existing generator `{target}`"));
        }
    };
    if target_exists && !force {
        anyhow::bail!("generator `{id}` already exists at `{target}`; pass --force to replace it");
    }
    if dunce::canonicalize(generators_dir)?.starts_with(dunce::canonicalize(&staged.path)?) {
        anyhow::bail!(
            "install destination `{generators_dir}` is inside source `{}`",
            staged.path
        );
    }
    let temporary = unique_directory_at(generators_dir, "qcg-install-temp")?;
    let mut temporary = CleanupPath::new(temporary);
    qcg_service::package::copy_dir_all(
        &staged.path,
        temporary.path().expect("temporary path must be armed"),
        limits,
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let copied_contract = Contract::load(temporary.path().expect("temporary path must be armed"))?;
    if copied_contract.manifest.generator.id != id {
        anyhow::bail!("copied generator manifest id does not match `{id}`");
    }
    app_registry(providers_path)?.validate_contract(&copied_contract)?;
    commit_install(
        temporary.path().expect("temporary path must be armed"),
        &target,
        target_exists,
    )?;
    temporary.disarm();
    Ok(id)
}

pub(crate) fn commit_install(
    temporary: &Utf8Path,
    target: &Utf8Path,
    replace_existing: bool,
) -> Result<()> {
    let parent = target
        .parent()
        .context("install target must have a parent directory")?;
    let backup = if replace_existing {
        let backup = loop {
            let candidate = unique_nonexistent_path(parent, "qcg-install-backup")?;
            match std::fs::rename(target, &candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to move existing generator `{target}`"));
                }
            }
        };
        Some(CleanupPath::new(backup))
    } else {
        None
    };

    if !replace_existing {
        match std::fs::symlink_metadata(target) {
            Ok(_) => anyhow::bail!("install target `{target}` appeared during staging"),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install target `{target}`"));
            }
        }
    }

    if let Err(error) = std::fs::rename(temporary, target) {
        let Some(mut backup) = backup else {
            return Err(error).with_context(|| format!("failed to commit generator `{target}`"));
        };
        let backup_path = backup
            .path()
            .map(|path| path.to_owned())
            .expect("backup path must be armed");
        match std::fs::rename(&backup_path, target) {
            Ok(()) => {
                backup.disarm();
                return Err(error)
                    .with_context(|| format!("failed to commit generator `{target}`"));
            }
            Err(restore_error) => {
                // Keep the backup when restoration itself fails so the existing install remains recoverable.
                backup.disarm();
                return Err(anyhow::anyhow!(
                    "failed to commit generator `{target}`: {error}; failed to restore existing generator: {restore_error}; backup remains at `{backup_path}`"
                ));
            }
        }
    }

    if let Some(mut backup) = backup {
        backup
            .cleanup()
            .with_context(|| format!("installed `{target}` but failed to remove backup"))?;
    }
    Ok(())
}

pub(crate) fn uninstall(id: &str, generators_dir: &Utf8Path, yes: bool) -> Result<()> {
    ensure_safe_install_id(id)?;
    let target = generators_dir.join(id);
    if !target.join("qcg.toml").exists() {
        anyhow::bail!("generator `{id}` is not installed under `{generators_dir}`");
    }
    if !yes {
        confirm_stdin(&format!("Uninstall generator `{id}`?"))?;
    }
    std::fs::remove_dir_all(&target)
        .with_context(|| format!("failed to remove generator `{target}`"))?;
    Ok(())
}

async fn stage_install_source(
    source: &str,
    expected_sha256: Option<&str>,
    signature: Option<(&str, &str)>,
    limits: &qcg_service::PackageLimits,
) -> Result<StagedInstall> {
    let mut cleanup = CleanupPaths::default();
    let source_path = if let Some(path) = source.strip_prefix("file://") {
        let path = Utf8PathBuf::from(path);
        if !path.exists() {
            anyhow::bail!("package file `{path}` was not found");
        }
        path
    } else if source.starts_with("http://") || source.starts_with("https://") {
        let response = reqwest::get(source).await?.error_for_status()?;
        if limits.max_archive_bytes.is_some_and(|limit| {
            response
                .content_length()
                .is_some_and(|length| length > limit)
        }) {
            anyhow::bail!(
                "remote package exceeds {} bytes",
                limits.max_archive_bytes.unwrap_or(u64::MAX)
            );
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let next = bytes
                .len()
                .checked_add(chunk.len())
                .context("remote package size overflowed")?;
            if limits
                .max_archive_bytes
                .is_some_and(|limit| next as u64 > limit)
            {
                anyhow::bail!(
                    "remote package exceeds {} bytes",
                    limits.max_archive_bytes.unwrap_or(u64::MAX)
                );
            }
            bytes.extend_from_slice(&chunk);
        }
        verify_package_bytes(&bytes, expected_sha256, signature)?;
        let temp_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir()).map_err(|path| {
            anyhow::anyhow!(
                "temporary directory path is not valid UTF-8: {}",
                path.display()
            )
        })?;
        let archive = unique_file_at(&temp_dir, "qcg-install-archive", "qcg")?;
        cleanup.push(archive.clone());
        std::fs::write(&archive, bytes)?;
        archive
    } else {
        Utf8PathBuf::from(source)
    };
    if source_path.is_dir() {
        if expected_sha256.is_some() || signature.is_some() {
            anyhow::bail!("checksum and signature verification require a package archive");
        }
        return Ok(StagedInstall {
            path: source_path,
            _cleanup: cleanup,
        });
    }
    if !source.starts_with("http://") && !source.starts_with("https://") {
        let bytes = qcg_policy::read_bounded(
            &source_path,
            limits
                .max_archive_bytes
                .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
        )?;
        verify_package_bytes(&bytes, expected_sha256, signature)?;
    }
    let stage = unique_stage_dir()?;
    cleanup.push(stage.clone());
    qcg_service::package::unpack_qcg(&source_path, &stage, limits)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(StagedInstall {
        path: stage,
        _cleanup: cleanup,
    })
}

pub(crate) fn verify_package_bytes(
    bytes: &[u8],
    expected_sha256: Option<&str>,
    signature: Option<(&str, &str)>,
) -> Result<()> {
    if let Some(expected) = expected_sha256 {
        let expected = expected.trim().to_ascii_lowercase();
        if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            anyhow::bail!("--sha256 must be a 64-character hexadecimal digest");
        }
        let actual = hex::encode(Sha256::digest(bytes));
        if actual != expected {
            anyhow::bail!("package SHA-256 mismatch: expected {expected}, got {actual}");
        }
    }
    if let Some((signature, public_key)) = signature {
        let signature = hex::decode(signature.trim())
            .context("--signature must be hexadecimal Ed25519 signature bytes")?;
        let public_key = hex::decode(public_key.trim())
            .context("--public-key must be hexadecimal Ed25519 public-key bytes")?;
        UnparsedPublicKey::new(&ED25519, public_key)
            .verify(bytes, &signature)
            .map_err(|_| anyhow::anyhow!("package Ed25519 signature verification failed"))?;
    }
    Ok(())
}

pub(crate) fn sign_package(output: &Utf8Path, bytes: &[u8], signing_key: &Utf8Path) -> Result<()> {
    let pkcs8 = qcg_policy::read_bounded(signing_key, Some(MAX_SIGNING_KEY_BYTES))
        .with_context(|| format!("failed to read Ed25519 PKCS#8 key `{signing_key}`"))?;
    let key = Ed25519KeyPair::from_pkcs8(&pkcs8)
        .map_err(|_| anyhow::anyhow!("`{signing_key}` is not a valid Ed25519 PKCS#8 key"))?;
    let signature_path = Utf8PathBuf::from(format!("{output}.sig"));
    let public_key_path = Utf8PathBuf::from(format!("{output}.pub"));
    std::fs::write(&signature_path, hex::encode(key.sign(bytes).as_ref()))?;
    std::fs::write(&public_key_path, hex::encode(key.public_key().as_ref()))?;
    println!("signature {signature_path}");
    println!("public_key {public_key_path}");
    Ok(())
}

fn unique_stage_dir() -> Result<Utf8PathBuf> {
    let parent = Utf8PathBuf::from_path_buf(std::env::temp_dir()).map_err(|path| {
        anyhow::anyhow!(
            "temporary directory path is not valid UTF-8: {}",
            path.display()
        )
    })?;
    unique_directory_at(&parent, "qcg-install-stage")
}

fn unique_directory_at(parent: &Utf8Path, prefix: &str) -> Result<Utf8PathBuf> {
    loop {
        let path = parent.join(format!(".{prefix}-{}", Uuid::now_v7()));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create temporary directory `{path}`"));
            }
        }
    }
}

fn unique_file_at(parent: &Utf8Path, prefix: &str, extension: &str) -> Result<Utf8PathBuf> {
    loop {
        let path = parent.join(format!(".{prefix}-{}.{}", Uuid::now_v7(), extension));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => return Ok(path),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create temporary file `{path}`"));
            }
        }
    }
}

fn unique_nonexistent_path(parent: &Utf8Path, prefix: &str) -> Result<Utf8PathBuf> {
    loop {
        let path = parent.join(format!(".{prefix}-{}", Uuid::now_v7()));
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(path),
            Ok(_) => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect temporary path `{path}`"));
            }
        }
    }
}
