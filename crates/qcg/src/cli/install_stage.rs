//! Registry install staging (E14): resolve, verify, and stage generator
//! closures without committing. Split from `install.rs` with no behavior
//! change (E14/C-5).

use anyhow::{Context, Result};

use camino::{Utf8Path, Utf8PathBuf};
use futures_util::StreamExt;
use qcg_contract::{Contract, Graph};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read as _};
use uuid::Uuid;

use super::plan::ensure_safe_install_id;

use super::install::path_exists;
use super::install::verify_package_bytes;
use super::install_commit::{CleanupPaths, finish_install, unique_stage_dir_in};

pub(crate) struct StagedInstall {
    pub(crate) path: Utf8PathBuf,
    // Parsed once at stage time from the staged bytes; threaded through
    // commit without re-opening or re-parsing the manifest (E14-1).
    pub(crate) contract: Contract,
    pub(crate) _cleanup: CleanupPaths,
}

/// Visiting stack key stores (id, requirement) for diagnostics, but cycle
/// detection compares ids only: the install target directory is keyed by id
/// alone, so a self-dependency with a different version still collides on
/// the same target dir and must fail closed (E14-3).
pub(crate) type Visiting = BTreeSet<(String, String)>;

pub(crate) fn visiting_contains(visiting: &Visiting, id: &str) -> bool {
    visiting.iter().any(|(member, _)| member == id)
}

#[derive(Debug)]
pub(crate) struct CycleError {
    message: String,
}

impl std::fmt::Display for CycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for CycleError {}

pub(crate) fn is_cycle_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<CycleError>())
}

pub(crate) fn cycle_error(message: String) -> anyhow::Error {
    anyhow::Error::new(CycleError { message })
}

/// Opens a file without following a terminal symlink on Unix (O_NOFOLLOW at
/// use time). Non-Unix pre-checks for a symlink.
pub(crate) fn open_manifest_nofollow(path: &Utf8Path) -> Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open manifest `{path}`"))
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                anyhow::bail!("refusing to open symbolic link `{path}`")
            }
            Ok(_) => std::fs::File::open(path)
                .with_context(|| format!("failed to open manifest `{path}`")),
            Err(error) => {
                Err(error).with_context(|| format!("failed to inspect manifest `{path}`"))
            }
        }
    }
}

/// Reads the manifest bytes ONCE from the opened handle (`read_to_end` from
/// the `File`) and parses the manifest from those exact bytes. No second
/// open happens between verification and use (E14-1).
pub(crate) fn read_manifest_bytes_pinned(
    manifest_path: &Utf8Path,
    limits: &qcg_service::PackageLimits,
) -> Result<String> {
    let mut file = open_manifest_nofollow(manifest_path)?;
    let mut bytes = Vec::new();
    if let Some(limit) = limits.max_metadata_bytes {
        file.take(limit.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .with_context(|| format!("failed to read manifest `{manifest_path}`"))?;
        if bytes.len() > limit {
            anyhow::bail!("manifest `{manifest_path}` exceeds {limit} bytes");
        }
    } else {
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read manifest `{manifest_path}`"))?;
    }
    String::from_utf8(bytes)
        .with_context(|| format!("manifest `{manifest_path}` is not valid UTF-8"))
}

/// Builds a `Contract` from already-pinned manifest bytes without
/// re-opening the file. Runs the public manifest validation and the
/// qcg_version gate so behavior matches `Contract::load` for the fields
/// installers rely on.
pub(crate) fn contract_from_pinned_bytes(root: &Utf8Path, source: &str) -> Result<Contract> {
    let manifest: qcg_contract::Manifest =
        toml::from_str(source).with_context(|| format!("failed to parse manifest in `{root}`"))?;
    manifest
        .validate()
        .map_err(|error| anyhow::anyhow!("invalid manifest in `{root}`: {error}"))?;
    let requirement = manifest.generator.qcg_version.trim();
    if requirement.is_empty() {
        anyhow::bail!(
            "generator `{}` must declare generator.qcg_version",
            manifest.generator.id
        );
    }
    let requirement = semver::VersionReq::parse(requirement)
        .with_context(|| format!("generator.qcg_version `{requirement}` is invalid"))?;
    let current = semver::Version::parse(env!("CARGO_PKG_VERSION"))
        .with_context(|| "qcg runtime version is invalid")?;
    if !requirement.matches(&current) {
        anyhow::bail!(
            "generator requires qcg_version `{requirement}`, runtime is `{}`",
            env!("CARGO_PKG_VERSION")
        );
    }
    let graph = Graph::build(&manifest)
        .map_err(|error| anyhow::anyhow!("invalid graph in `{root}`: {error}"))?;
    let sha256 = hex::encode(Sha256::digest(source.as_bytes()));
    Ok(Contract {
        root: root.to_path_buf(),
        manifest,
        graph,
        sha256,
    })
}

/// Verified contract load for installed or staged packages: verifies the
/// SBOM inventory first, then loads pinned bytes and cross-checks the pinned
/// manifest hash against the SBOM so a swap between verify and use fails
/// closed (E14-1).
pub(crate) fn load_verified_contract(
    root: &Utf8Path,
    limits: &qcg_service::PackageLimits,
) -> Result<Contract> {
    qcg_service::package::verify_installed_package(root, limits).map_err(|error| {
        anyhow::anyhow!("installed generator failed inventory verification: {error}")
    })?;
    let manifest_path = root.join("qcg.toml");
    let source = read_manifest_bytes_pinned(&manifest_path, limits)?;
    let contract = contract_from_pinned_bytes(root, &source)?;
    // Cross-check: the pinned bytes must hash to the SBOM-listed digest for
    // qcg.toml, proving the bytes we parsed are the bytes that verified.
    let sbom_path = root.join("QCG-SBOM.spdx.json");
    let sbom_bytes = qcg_fs::read_bounded(&sbom_path, limits.max_metadata_bytes)
        .with_context(|| format!("package is missing QCG-SBOM.spdx.json in `{root}`"))?;
    let sbom: serde_json::Value = serde_json::from_slice(&sbom_bytes)
        .with_context(|| format!("package SBOM in `{root}` is not valid JSON"))?;
    let expected = sbom
        .pointer("/files")
        .and_then(|files| files.as_array())
        .and_then(|files| {
            files.iter().find_map(|file| {
                if file.get("fileName").and_then(|name| name.as_str()) == Some("qcg.toml") {
                    file.pointer("/checksums/0/checksumValue")
                        .and_then(|digest| digest.as_str())
                } else {
                    None
                }
            })
        })
        .with_context(|| format!("package SBOM in `{root}` lists no qcg.toml digest"))?;
    let actual = hex::encode(Sha256::digest(source.as_bytes()));
    if actual != expected {
        anyhow::bail!(
            "manifest in `{root}` changed between verification and use (expected {expected}, got {actual})"
        );
    }
    Ok(contract)
}

/// Loads the staged manifest: SBOM is REQUIRED (fail-closed, E14). The
/// raw-directory pinned-only compat is removed: a staged package without
/// supply-chain metadata fails instead of verifying by hash alone, so an
/// SBOM-stripped package cannot downgrade to hash-only verification.
/// Direct directory sources must carry an SBOM (produce one via `qcg
/// package`, which always emits it) or the install fails closed.
pub(crate) fn load_staged_manifest(
    staged: &Utf8Path,
    limits: &qcg_service::PackageLimits,
) -> Result<Contract> {
    if !path_exists(&staged.join("QCG-SBOM.spdx.json"))? {
        anyhow::bail!(
            "package in `{staged}` is missing QCG-SBOM.spdx.json; refusing to install without supply-chain metadata"
        );
    }
    load_verified_contract(staged, limits)
}

/// Loads the installed manifest: SBOM is REQUIRED (fail-closed, E14).
/// Installed packages without supply-chain metadata fail inventory
/// verification instead of falling back to pinned-only loads.
pub(crate) fn load_installed_manifest(
    root: &Utf8Path,
    limits: &qcg_service::PackageLimits,
) -> Result<qcg_contract::Manifest> {
    if !path_exists(&root.join("QCG-SBOM.spdx.json"))? {
        anyhow::bail!(
            "installed generator in `{root}` is missing QCG-SBOM.spdx.json; refusing to trust it without supply-chain metadata"
        );
    }
    Ok(load_verified_contract(root, limits)?.manifest)
}

/// Shared install options so the recursive install chain does not need
/// seven-argument signatures.
#[derive(Clone, Copy)]
pub(crate) struct InstallConfig<'a> {
    pub(crate) providers_path: Option<&'a Utf8Path>,
    pub(crate) generators_dir: &'a Utf8Path,
    pub(crate) yes: bool,
    pub(crate) force: bool,
    pub(crate) limits: &'a qcg_service::PackageLimits,
}

/// Install a registry-resolved package plus its dependencies.
pub(crate) fn install_registry_package<'a>(
    config: InstallConfig<'a>,
    id: &'a str,
    requirement: &'a semver::VersionReq,
    visiting: &'a mut Visiting,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + 'a>> {
    Box::pin(async move {
        let InstallConfig {
            generators_dir,
            force,
            limits,
            ..
        } = config;
        ensure_safe_install_id(id)?;
        if visiting_contains(visiting, id) {
            return Err(cycle_error(format!(
                "dependency cycle detected at generator `{id}`"
            )));
        }
        // Direct self-dependency fires here too: the parent id is already on
        // the stack when its own dependency list is walked.
        // E14-1 single verified load: version and manifest come from the
        // SAME pinned bytes, never two opens (no TOCTOU between
        // version-check and use).
        if let Some((installed, manifest)) =
            load_installed_version_and_manifest(generators_dir, id, limits)?
        {
            if requirement.matches(&installed) {
                println!("generator `{id}@{installed}` already satisfies `{requirement}`");
                // A satisfied version is not a trustworthy install: the
                // single verified load above already proved inventory, and
                // the manifest below is from those same bytes (no second
                // open between verify and use, E14-1). Cycle detection stays
                // active for the walk with a version-qualified key (E14-3).
                // E14 satisfied-parent single-commit: the FULL closure is
                // staged before ANY commit (same stage-full-then-commit-once
                // as the fresh path below). A failed dependency refuses
                // pre-commit instead of leaving a committed parent behind
                // with a rerun demand; a satisfied parent is never removed
                // here — not even on cycle errors — since it pre-existed
                // this run (user data).
                let key = (id.to_string(), installed.to_string());
                visiting.insert(key.clone());
                let staged_closure =
                    match stage_closure_for_manifest(config, &manifest, visiting).await {
                        Ok(staged) => staged,
                        Err(error) => {
                            visiting.remove(&key);
                            return Err(error);
                        }
                    };
                // Single commit phase: dependencies only (leaf order from
                // staging), parent last is SKIPPED since it pre-existed. A
                // failed dependency commit never leaves a committed parent
                // behind (there is none to leave); valid standalone
                // dependency installs remain, never an incomplete parent.
                for dep_staged in staged_closure {
                    let dep_id = dep_staged.contract.manifest.generator.id.clone();
                    if let Err(error) = finish_install(
                        config.providers_path,
                        dep_staged,
                        generators_dir,
                        config.yes,
                        config.force,
                        limits,
                    ) {
                        visiting.remove(&key);
                        if is_cycle_error(&error) {
                            return Err(anyhow::anyhow!(
                                "generator `{id}` dependency cycle committing `{dep_id}`: {error}; refusing to commit parent"
                            ));
                        }
                        return Err(anyhow::anyhow!(
                            "dependency closure cannot be satisfied for generator `{id}` committing `{dep_id}`: {error}; refusing to commit parent"
                        ));
                    }
                }
                visiting.remove(&key);
                return Ok(id.to_string());
            }
            if !force {
                anyhow::bail!(
                    "generator `{id}@{installed}` is installed but does not satisfy `{requirement}`; pass --force to replace it"
                );
            }
        }
        // Fresh installs resolve first; the version-qualified key is pushed
        // by the inner step once the exact version is known.
        install_registry_package_inner(config, id, requirement, visiting).await
    })
}

pub(crate) async fn install_registry_package_inner<'a>(
    config: InstallConfig<'a>,
    id: &'a str,
    requirement: &'a semver::VersionReq,
    visiting: &'a mut Visiting,
) -> Result<String> {
    let InstallConfig {
        providers_path,
        generators_dir,
        yes,
        force,
        limits,
    } = config;
    let home = crate::registry::home_dir()?;
    let registries = crate::registry::load_registries(&home)?;
    let keys = crate::registry::load_trusted_keys(&home)?;
    let resolved = crate::registry::resolve(&registries, id, requirement).await?;
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
    // The staged contract was parsed once at stage time; verify it claims
    // the resolved registry identity before staging the closure. Hash and
    // signature alone prove byte integrity, not id and version.
    if staged.contract.manifest.generator.id != id {
        anyhow::bail!(
            "registry package id mismatch: requested `{id}` but archive contains `{}`",
            staged.contract.manifest.generator.id
        );
    }
    if staged.contract.manifest.generator.version != resolved.entry.version {
        anyhow::bail!(
            "registry package version mismatch for `{id}`: resolved `{}` but archive contains `{}`",
            resolved.entry.version,
            staged.contract.manifest.generator.version
        );
    }
    let resolved_version = resolved.entry.version.clone();
    if visiting_contains(visiting, id) {
        return Err(cycle_error(format!(
            "dependency cycle detected at generator `{id}`"
        )));
    }
    let key = (id.to_string(), resolved_version.clone());
    visiting.insert(key.clone());
    // Resolve and stage the FULL closure (parent plus transitive
    // dependencies) before any commit. A failed dependency refuses the
    // parent pre-commit instead of leaving a committed parent behind with
    // a rerun demand. The documented intent (no silent incomplete closure)
    // is kept, but the order is inverted: stage all, then commit once.
    let staged_closure = match stage_closure_for_manifest(
        config,
        &staged.contract.manifest,
        visiting,
    )
    .await
    {
        Ok(staged) => staged,
        Err(error) => {
            visiting.remove(&key);
            if is_cycle_error(&error) {
                return Err(anyhow::anyhow!(
                    "generator `{id}` dependency cycle: {error}; refusing to commit parent, fix the cycle and retry"
                ));
            }
            return Err(anyhow::anyhow!(
                "dependency closure cannot be satisfied for generator `{id}`: {error}; refusing to commit parent"
            ));
        }
    };
    // Single commit phase: dependencies first (leaf order from staging),
    // parent last. A failed dependency commit never leaves a committed
    // parent behind. A parent commit failure after dependencies leaves
    // valid standalone dependency installs, never an incomplete parent.
    for dep_staged in staged_closure {
        let dep_id = dep_staged.contract.manifest.generator.id.clone();
        if let Err(error) = finish_install(
            providers_path,
            dep_staged,
            generators_dir,
            yes,
            force,
            limits,
        ) {
            visiting.remove(&key);
            if is_cycle_error(&error) {
                return Err(anyhow::anyhow!(
                    "generator `{id}` dependency cycle committing `{dep_id}`: {error}; refusing to commit parent"
                ));
            }
            return Err(anyhow::anyhow!(
                "dependency closure cannot be satisfied for generator `{id}` committing `{dep_id}`: {error}; refusing to commit parent"
            ));
        }
    }
    let installed_id =
        match finish_install(providers_path, staged, generators_dir, yes, force, limits) {
            Ok(id) => id,
            Err(error) => {
                visiting.remove(&key);
                return Err(error);
            }
        };
    visiting.remove(&key);
    Ok(installed_id)
}

/// Stages the full dependency closure of `manifest` without committing
/// anything: every missing transitive dependency is resolved and staged
/// into private staging, already-installed satisfying packages are verified
/// and their own closures walked. The returned stages are in commit order
/// (leaf dependencies first). A failure here happens before any parent
/// commit, so a failed dependency never leaves a committed parent behind.
/// Pre-validates ALL dependencies (safe ids + requirement parsing + cycle
/// check) before staging ANY of them, so a validation failure leaves no
/// partial staging residue (E14). Cycle detection uses `visiting` (id-based
/// for diagnostics, with version-qualified keys for the install-dir
/// collision).
pub(crate) fn stage_closure_for_manifest<'a>(
    config: InstallConfig<'a>,
    manifest: &'a qcg_contract::Manifest,
    visiting: &'a mut Visiting,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<StagedInstall>>> + Send + 'a>> {
    Box::pin(async move {
        // Single validation pass (E14): parse every requirement once and
        // reuse the parsed form below, so validation and staging never
        // parse twice. ALL deps validate before ANY staging begins (no
        // partial residue on validation failure).
        let mut validated: Vec<(&String, semver::VersionReq)> =
            Vec::with_capacity(manifest.dependencies.len());
        for (dependency, requirement) in &manifest.dependencies {
            ensure_safe_install_id(dependency)?;
            let parsed = semver::VersionReq::parse(requirement).with_context(|| {
                format!("dependency `{dependency}` version requirement is invalid")
            })?;
            if visiting_contains(visiting, dependency) {
                return Err(cycle_error(format!(
                    "dependency cycle detected at generator `{dependency}`"
                )));
            }
            validated.push((dependency, parsed));
        }
        let mut staged = Vec::new();
        for (dependency, requirement) in validated {
            // E14-1 single verified load: version + manifest from the SAME
            // pinned bytes, never two opens.
            if let Some((installed, installed_manifest)) = load_installed_version_and_manifest(
                config.generators_dir,
                dependency,
                config.limits,
            )? {
                if requirement.matches(&installed) {
                    // Verified reuse, then walk its own closure for transitive
                    // missing sets without staging the already-installed node.
                    let key = (dependency.clone(), installed.to_string());
                    visiting.insert(key.clone());
                    let transitive =
                        stage_closure_for_manifest(config, &installed_manifest, visiting).await;
                    visiting.remove(&key);
                    staged.extend(transitive?);
                    continue;
                }
                if !config.force {
                    anyhow::bail!(
                        "generator `{dependency}@{installed}` is installed but does not satisfy `{requirement}`; pass --force to replace it"
                    );
                }
            }
            let dep_staged =
                stage_single_with_closure(config, dependency, &requirement, visiting).await?;
            staged.extend(dep_staged);
        }
        Ok(staged)
    })
}

/// Resolves, verifies, and stages a single missing registry package plus
/// its own transitive closure, returning stages in commit order (deps
/// first, the package itself last). No commit happens here.
pub(crate) fn stage_single_with_closure<'a>(
    config: InstallConfig<'a>,
    id: &'a str,
    requirement: &'a semver::VersionReq,
    visiting: &'a mut Visiting,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<StagedInstall>>> + Send + 'a>> {
    Box::pin(async move {
        ensure_safe_install_id(id)?;
        let home = crate::registry::home_dir()?;
        let registries = crate::registry::load_registries(&home)?;
        let keys = crate::registry::load_trusted_keys(&home)?;
        let resolved = crate::registry::resolve(&registries, id, requirement).await?;
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
            config.limits,
        )
        .await?;
        // The staged contract was parsed once at stage time; verify it claims
        // the resolved identity before staging its own closure.
        if staged.contract.manifest.generator.id != id {
            anyhow::bail!(
                "registry package id mismatch: requested `{id}` but archive contains `{}`",
                staged.contract.manifest.generator.id
            );
        }
        if staged.contract.manifest.generator.version != resolved.entry.version {
            anyhow::bail!(
                "registry package version mismatch for `{id}`: resolved `{}` but archive contains `{}`",
                resolved.entry.version,
                staged.contract.manifest.generator.version
            );
        }
        if visiting_contains(visiting, id) {
            return Err(cycle_error(format!(
                "dependency cycle detected at generator `{id}`"
            )));
        }
        let key = (id.to_string(), resolved.entry.version.clone());
        visiting.insert(key.clone());
        let staged_manifest = staged.contract.manifest.clone();
        let staged_closure = stage_closure_for_manifest(config, &staged_manifest, visiting).await;
        visiting.remove(&key);
        let mut transitive = staged_closure?;
        // Deps first, self last: committing in order never leaves a parent
        // without its dependencies staged.
        transitive.push(staged);
        Ok(transitive)
    })
}

/// Single verified load returning the installed version and manifest from
/// the SAME pinned bytes (E14-1 TOCTOU fix): the manifest is read once,
/// verified (SBOM inventory + pinned hash cross-check), and parsed from
/// those exact bytes. The version is compared from the same bytes, never
/// from a second open between verification and use. Returns `None` when no
/// manifest exists.
pub(crate) fn load_installed_version_and_manifest(
    generators_dir: &Utf8Path,
    id: &str,
    limits: &qcg_service::PackageLimits,
) -> Result<Option<(semver::Version, qcg_contract::Manifest)>> {
    let manifest_path = generators_dir.join(id).join("qcg.toml");
    if !path_exists(&manifest_path)? {
        return Ok(None);
    }
    // Verified load (SBOM required, see `load_installed_manifest`): the
    // version gate must not trust unverified bytes (E14).
    let manifest = load_installed_manifest(&generators_dir.join(id), limits)?;
    let version = semver::Version::parse(&manifest.generator.version)
        .with_context(|| format!("installed generator `{id}` has an invalid version"))?;
    Ok(Some((version, manifest)))
}

pub(crate) async fn stage_install_source(
    source: &str,
    expected_sha256: Option<&str>,
    signature: Option<(&str, &str)>,
    limits: &qcg_service::PackageLimits,
) -> Result<StagedInstall> {
    let mut cleanup = CleanupPaths::default();
    // All archive bytes stage inside a private per-process subdir (0700),
    // never directly in the shared tmp (E14-6).
    let private_dir = private_stage_dir()?;
    cleanup.push(private_dir.clone());
    let mut source_path = if let Some(path) = source.strip_prefix("file://") {
        let path = Utf8PathBuf::from(path);
        if !path_exists(&path)? {
            anyhow::bail!("package file `{path}` was not found");
        }
        path
    } else if source.starts_with("http://") || source.starts_with("https://") {
        let response = reqwest::get(source).await?.error_for_status()?;
        if let Some(limit) = limits.max_archive_bytes
            && response
                .content_length()
                .is_some_and(|length| length > limit)
        {
            anyhow::bail!("remote package exceeds {limit} bytes");
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let next = bytes
                .len()
                .checked_add(chunk.len())
                .context("remote package size overflowed")?;
            if let Some(limit) = limits.max_archive_bytes
                && next as u64 > limit
            {
                anyhow::bail!("remote package exceeds {limit} bytes");
            }
            bytes.extend_from_slice(&chunk);
        }
        verify_package_bytes(&bytes, expected_sha256, signature)?;
        // Single create_new open holds the verified bytes; no re-open
        // window between create and write (E14-6).
        write_archive_bytes_private(&private_dir, &bytes)?
    } else {
        Utf8PathBuf::from(source)
    };
    if path_exists(&source_path)?
        && std::fs::symlink_metadata(&source_path)?
            .file_type()
            .is_dir()
    {
        if expected_sha256.is_some() || signature.is_some() {
            anyhow::bail!("checksum and signature verification require a package archive");
        }
        // Verify directory source names/contents before staging, with the
        // same name validation as other sources (E14-7). The manifest id
        // and every dependency name must be safe install ids, and the tree
        // must contain no symlinks or unsafe paths; malicious dep names
        // cannot reach the registry.
        validate_direct_source_dir(&source_path, limits)?;
        // Never return the live directory: copy the verified tree into
        // private staging and return the staged copy, so a mutation of the
        // live source between validation and commit cannot redirect the
        // install (E14). The manifest is parsed once from the staged copy.
        let stage = unique_stage_dir_in(&private_dir)?;
        cleanup.push(stage.clone());
        qcg_service::package::copy_dir_all(&source_path, &stage, limits)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let contract = load_staged_manifest(&stage, limits)?;
        return Ok(StagedInstall {
            path: stage,
            contract,
            _cleanup: cleanup,
        });
    }
    if !source.starts_with("http://") && !source.starts_with("https://") {
        // file:// or plain path to an archive file.
        let archive_limit = limits
            .max_archive_bytes
            .map(|limit| {
                usize::try_from(limit).map_err(|_| {
                    std::io::Error::other("package archive limit is not representable")
                })
            })
            .transpose()?;
        // Pin with a single handle: open O_NOFOLLOW, read_to_end from the
        // File, verify those bytes, and stage the same bytes (E14-1).
        let mut handle = open_archive_nofollow(&source_path)?;
        let mut bytes = Vec::new();
        if let Some(limit) = archive_limit {
            use std::io::Read as _;
            handle
                .take(limit.saturating_add(1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > limit {
                anyhow::bail!("package archive exceeds {limit} bytes");
            }
        } else {
            use std::io::Read as _;
            handle.read_to_end(&mut bytes)?;
        }
        verify_package_bytes(&bytes, expected_sha256, signature)?;
        // Unpack from a private copy of the verified bytes, not by
        // re-opening the source path: the source file could be swapped
        // between verification and unpacking (E14).
        let archive = write_archive_bytes_private(&private_dir, &bytes)?;
        source_path = archive;
    }
    let stage = unique_stage_dir_in(&private_dir)?;
    cleanup.push(stage.clone());
    qcg_service::package::unpack_qcg(&source_path, &stage, limits)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    // Parse the staged manifest once here; callers thread `contract`
    // through commit without re-opening or re-parsing (E14-1).
    let contract = load_staged_manifest(&stage, limits)?;
    Ok(StagedInstall {
        path: stage,
        contract,
        _cleanup: cleanup,
    })
}

/// Validates a direct directory source before it is staged (E14-7).
/// SBOM is REQUIRED (fail-closed, E14): the source must carry
/// supply-chain metadata, verified here via the same verified load the
/// staging uses. The manifest id and every dependency name must be safe
/// install ids, and the tree must contain no symlinks or unsafe paths;
/// malicious dep names cannot reach the registry.
pub(crate) fn validate_direct_source_dir(
    source: &Utf8Path,
    limits: &qcg_service::PackageLimits,
) -> Result<()> {
    // Verified load (SBOM required): a directory source without an SBOM
    // fails closed here, never staging via pinned-only compat.
    let contract = load_verified_contract(source, limits).map_err(|error| {
        anyhow::anyhow!(
            "direct directory source in `{source}` failed supply-chain verification: {error}"
        )
    })?;
    ensure_safe_install_id(&contract.manifest.generator.id)?;
    for dependency in contract.manifest.dependencies.keys() {
        ensure_safe_install_id(dependency)?;
    }
    // Reject symlinks and unsafe paths up front, mirroring copy_dir_all.
    for entry in qcg_fs::WalkDir::new(source) {
        let entry = entry.with_context(|| format!("failed to walk package source `{source}`"))?;
        if entry.file_type().is_symlink() {
            anyhow::bail!("package source contains a symbolic link: {}", entry.path());
        }
        let rel = entry
            .path()
            .strip_prefix(source)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        if rel.as_str().is_empty() {
            continue;
        }
        if !qcg_policy::is_safe_relative_path(&qcg_policy::portable_relative_path(rel)) {
            anyhow::bail!("package source contains an unsafe path `{rel}`");
        }
    }
    Ok(())
}

/// Opens an archive file O_NOFOLLOW at use time.
pub(crate) fn open_archive_nofollow(path: &Utf8Path) -> Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open package archive `{path}`"))
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                anyhow::bail!("refusing to open symbolic link `{path}`")
            }
            Ok(_) => std::fs::File::open(path)
                .with_context(|| format!("failed to open package archive `{path}`")),
            Err(error) => Err(error).with_context(|| format!("failed to inspect `{path}`")),
        }
    }
}

/// Creates a private per-process staging subdir (0700) under the OS temp
/// dir. Archives and unpacked stages live here instead of the shared tmp so
/// a world-writable parent never hosts them (E14-6).
pub(crate) fn private_stage_dir() -> Result<Utf8PathBuf> {
    let parent = Utf8PathBuf::from_path_buf(std::env::temp_dir()).map_err(|path| {
        anyhow::anyhow!(
            "temporary directory path is not valid UTF-8: {}",
            path.display()
        )
    })?;
    let pid = std::process::id();
    loop {
        let path = parent.join(format!(".qcg-install-private-{pid}-{}", Uuid::now_v7()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&path) {
                Ok(()) => return Ok(path),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to create private staging `{path}`"));
                }
            }
        }
        #[cfg(not(unix))]
        {
            // No owner-only directory mode exists here; uniqueness plus the
            // shared-temp warning in the module docs is the boundary (E14).
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(path),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to create private staging `{path}`"));
                }
            }
        }
    }
}

/// Writes verified archive bytes into the private dir with a single
/// create_new open (no re-open window, E14-6). Unix creates the file with
/// mode `0600` atomically so no umask window exposes the bytes between
/// creation and a later chmod.
pub(crate) fn write_archive_bytes_private(dir: &Utf8Path, bytes: &[u8]) -> Result<Utf8PathBuf> {
    let pid = std::process::id();
    for _ in 0..100 {
        let path = dir.join(format!(".qcg-install-archive-{pid}-{}.qcg", Uuid::now_v7()));
        #[cfg(unix)]
        let open_result = {
            use std::os::unix::fs::OpenOptionsExt as _;
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
        };
        #[cfg(not(unix))]
        let open_result = OpenOptions::new().write(true).create_new(true).open(&path);
        match open_result {
            Ok(mut file) => {
                use std::io::Write as _;
                file.write_all(bytes)
                    .with_context(|| format!("failed to stage archive `{path}`"))?;
                file.sync_all()
                    .with_context(|| format!("failed to sync archive `{path}`"))?;
                drop(file);
                // Mode was set atomically at creation on Unix; non-Unix
                // has no mode bits to set (E14/E15).
                return Ok(path);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create archive staging in `{dir}`"));
            }
        }
    }
    Err(anyhow::anyhow!("failed to stage archive bytes in `{dir}`"))
}
