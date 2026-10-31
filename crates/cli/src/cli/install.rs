//! Registry install entry (E14): the `install` command plus repair sweeps,
//! package byte verification, and tests. Resolution/staging live in
//! `install_stage.rs`; the commit phase, locks, backups, and uninstall live
//! in `install_commit.rs`. Split with no behavior change (E14/C-5).

use anyhow::{Context, Result};

use aws_lc_rs::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use camino::{Utf8Path, Utf8PathBuf};

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use std::io::ErrorKind;

use super::install_commit;
use super::plan::ensure_safe_install_id;

use policy::MAX_SIGNING_KEY_BYTES;

/// Test-only re-exports: the native tests below (and `main.rs` tests) reach
/// past the split through `install::` instead of naming submodules.
#[cfg(test)]
pub(crate) use super::install_commit::{
    CleanupPaths, check_no_replace_target, commit_install, finish_install, per_id_lock_name,
};
pub(crate) use super::install_commit::{finish_closure, remove_owned_path, uninstall};
pub(crate) use super::install_stage::{
    InstallConfig, Selected, install_registry_package, is_cycle_error, stage_closure_for_manifest,
    stage_install_source,
};
#[cfg(test)]
pub(crate) use super::install_stage::{StagedInstall, load_verified_contract};

pub(crate) struct InstallVerification<'a> {
    pub(crate) sha256: Option<&'a str>,
    pub(crate) signature: Option<&'a str>,
    pub(crate) public_key: Option<&'a str>,
}

/// Best-effort removal of stale install staging under `parent` matching
/// `prefix` (`.install-temp-*` under `generators_dir`; `.install-stage-*`,
/// `.install-archive-*`, and `.install-private-*` under the OS temp dir —
/// backups are never reaped here, they go through the recovery-aware backup
/// sweep). The 1h age gate applies to every entry including same-process
/// ones: the pid only proves which process created an entry, never that it
/// is dead, so a concurrent install's fresh staging is never touched
/// (E14-8). Files and directories are both reaped; symlinks are never
/// followed — a stale planted link is unlinked itself, never its target.
/// Removal failures are collected and returned, never silently squashed
/// (E14-11).
pub(crate) fn reap_install_tmps_before(
    parent: &Utf8Path,
    prefix: &str,
    cutoff: std::time::SystemTime,
) -> Result<()> {
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to scan `{parent}` for stale installs"));
        }
    };
    let mut failures = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                failures.push(format!("unreadable directory entry in `{parent}`: {error}"));
                continue;
            }
        };
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            // Non-UTF8 names never match a sweep prefix via lossy conversion (E14).
            continue;
        };
        if !name.starts_with(prefix) || name.starts_with(".install-backup-") {
            continue;
        }
        // Symlinks are never followed. Planted links under our temp prefixes
        // are reaped via `symlink_metadata` + `remove_file` (which unlinks
        // the link itself, never the target), so no permanent planted-link
        // residue remains (E14). `remove_owned_path` below uses
        // `symlink_metadata` (never follows) and removes files (including
        // symlinks) via `remove_file` or directories via `remove_dir_all`,
        // so unlinking a symlink is safe.
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(error) => {
                failures.push(format!("failed to inspect `{name}`: {error}"));
                continue;
            }
        };
        // For symlinks, use symlink_metadata (never follows) for the age
        // gate: `entry.metadata()` would follow the link and could inspect
        // outside the install tree. A symlink whose own metadata cannot be
        // read fails closed (kept: unreadable age never proves staleness).
        // The age gate applies to every entry including same-process ones:
        // the pid only proves which process created an entry, never that it
        // is dead, so a concurrent install in this process must not lose
        // its live staging to a sibling's sweep.
        let is_symlink = kind.is_symlink();
        // Never follow symlinks for the age gate: use the link's own
        // metadata, not the target's.
        let old_enough = (if is_symlink {
            std::fs::symlink_metadata(entry.path())
        } else {
            entry.metadata()
        })
        .and_then(|metadata| metadata.modified())
        .map(|mtime| mtime <= cutoff)
        .unwrap_or(false);
        if !old_enough {
            continue;
        }
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            anyhow::anyhow!("stale install path is not valid UTF-8: {}", path.display())
        })?;
        if let Err(error) = remove_owned_path(&path) {
            failures.push(format!("failed to remove stale install `{path}`: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "failed to reap stale installs in `{parent}`: {}",
            failures.join("; ")
        ))
    }
}

pub(crate) fn reap_stale_install_tmps(generators_dir: &Utf8Path) -> Result<()> {
    // Recovery runs before any sweep (F14-02): a missing target with a
    // backup sibling restores instead of being deleted as stale. The sweep
    // below never deletes a backup whose target is missing.
    recover_generators_backups(generators_dir)?;
    // A clock behind the directory mtimes would make everything look
    // young; saturating to "now" fails the sweep closed (removes nothing)
    // instead of wiping live staging. Fresh entries always survive,
    // including same-process ones: only age proves staleness.
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(3600))
        .unwrap_or(std::time::SystemTime::now());
    reap_install_tmps_before(generators_dir, ".install-temp-", cutoff)?;
    reap_install_backup_siblings(generators_dir, cutoff)?;
    if let Ok(parent) = Utf8PathBuf::from_path_buf(std::env::temp_dir()) {
        reap_install_tmps_before(&parent, ".install-stage-", cutoff)?;
        reap_install_tmps_before(&parent, ".install-archive-", cutoff)?;
        reap_install_tmps_before(&parent, ".install-private-", cutoff)?;
    }
    Ok(())
}

/// Recovery precedes sweep and surfaces ambiguity/validation failures.
fn recover_generators_backups(generators_dir: &Utf8Path) -> Result<()> {
    let ids = install_commit::backup_records(generators_dir)?
        .into_iter()
        .map(|record| record.id)
        .collect::<std::collections::BTreeSet<_>>();
    for id in ids {
        install_commit::with_backup_maintenance_lock(generators_dir, &id, || {
            install_commit::recover_interrupted_backup(
                &generators_dir.join(&id),
                &service::PackageLimits::default(),
            )
        })?;
    }
    Ok(())
}

/// Age is not enough: only a verified, durable current target authorizes
/// removal. Unknown records/legacy backups remain for operator inspection.
fn reap_install_backup_siblings(
    generators_dir: &Utf8Path,
    cutoff: std::time::SystemTime,
) -> Result<()> {
    let mut failures = Vec::new();
    for record in install_commit::backup_records(generators_dir)? {
        let result =
            install_commit::with_backup_maintenance_lock(generators_dir, &record.id, || {
                let target = generators_dir.join(&record.id);
                match std::fs::symlink_metadata(&target) {
                    Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error.into()),
                    Ok(_) => {}
                }
                if !install_commit::backup_freshness(&record).is_some_and(|mtime| mtime <= cutoff) {
                    return Ok(());
                }
                install_commit::verify_backup_identity(
                    &target,
                    &record.id,
                    &service::PackageLimits::default(),
                )
                .context("recovery cleanup pending; current target is not verified")?;
                install_commit::fsync_parent_dir(&target)?;
                install_commit::remove_backup_record(&record, &service::PackageLimits::default())
            });
        if let Err(error) = result {
            failures.push(format!("{}: {error:#}", record.sidecar));
        }
    }
    anyhow::ensure!(
        failures.is_empty(),
        "failed to reap recovery records: {}",
        failures.join("; ")
    );
    Ok(())
}

pub(crate) async fn install(
    providers_path: Option<&Utf8Path>,
    source: &str,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
    verification: InstallVerification<'_>,
    limits: &service::PackageLimits,
) -> Result<String> {
    reap_stale_install_tmps(generators_dir)?;
    // Classify URL sources before filesystem metadata: on Windows, URL
    // strings containing `:` are invalid paths and must not abort dispatch.
    let source_is_path_or_url = source.starts_with("file://")
        || source.starts_with("http://")
        || source.starts_with("https://")
        || path_exists(Utf8Path::new(source))?;
    if let Some((id, requirement)) = crate::registry::split_id_source(source)
        && !source_is_path_or_url
    {
        let requirement = semver::VersionReq::parse(&requirement)
            .with_context(|| format!("version requirement `{requirement}` is invalid"))?;
        ensure_safe_install_id(&id)?;
        let config = InstallConfig {
            providers_path,
            generators_dir,
            yes,
            force,
            limits,
        };
        return install_registry_package(
            config,
            &id,
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
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
    // The staged contract was parsed once at stage time; the full closure
    // is resolved and staged before any commit. A failed dependency refuses
    // pre-commit instead of leaving a committed parent behind with a rerun
    // demand. A direct install is not exempt from the closure invariant.
    let config = InstallConfig {
        providers_path,
        generators_dir,
        yes,
        force,
        limits,
    };
    let parent_id = staged.contract.manifest.generator.id.clone();
    let mut visiting = BTreeSet::new();
    let mut selected = Selected::new();
    visiting.insert((
        parent_id.clone(),
        staged.contract.manifest.generator.version.clone(),
    ));
    let staged_closure = match stage_closure_for_manifest(
        config,
        &staged.contract.manifest,
        &mut visiting,
        &mut selected,
    )
    .await
    {
        Ok(staged) => staged,
        Err(error) => {
            if is_cycle_error(&error) {
                return Err(anyhow::anyhow!(
                    "generator `{parent_id}` dependency cycle: {error}; refusing to commit parent, fix the cycle and retry"
                ));
            }
            return Err(anyhow::anyhow!(
                "dependency closure cannot be satisfied for generator `{parent_id}`: {error}; refusing to commit parent"
            ));
        }
    };
    let mut stages = staged_closure;
    stages.push(staged);
    finish_closure(config, stages, &selected)
        .with_context(|| format!("dependency closure cannot be satisfied for generator `{parent_id}`; refusing to commit parent"))?;
    Ok(parent_id)
}

/// Checks existence without squashing IO errors: `Path::exists` returns
/// false on permission errors, hiding failures (E14-11). NotFound maps to
/// false; any other error propagates fail-closed.
pub(crate) fn path_exists(path: &Utf8Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to inspect path `{path}`")),
    }
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
    let pkcs8 = files::read_bounded(signing_key, Some(MAX_SIGNING_KEY_BYTES))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::install_commit;

    static ENV_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    /// `file://` URL with forward slashes: raw backslashes would parse as
    /// TOML escapes inside index files on Windows.
    fn file_url(path: &Utf8Path) -> String {
        format!("file://{}", path.as_str().replace('\\', "/"))
    }

    /// A unique temp root for one test. `name` names the case so a leaked
    /// directory stays attributable to the test that created it.
    fn test_root(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("install-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8")
    }

    /// The install options every test uses: no providers file, no
    /// confirmation prompts, no forced replacement, and the given package
    /// limits. A test that needs another mode overrides only that field.
    fn test_config<'a>(
        limits: &'a service::PackageLimits,
        generators_dir: &'a Utf8Path,
    ) -> InstallConfig<'a> {
        InstallConfig {
            providers_path: None,
            generators_dir,
            yes: false,
            force: false,
            limits,
        }
    }

    /// No digest and no signature: a local directory source is verified by
    /// staging against its own contents, never against a pinned digest.
    fn test_verification() -> InstallVerification<'static> {
        InstallVerification {
            sha256: None,
            signature: None,
            public_key: None,
        }
    }

    fn write_installed(dir: &Utf8Path, id: &str, dependency: &str) {
        write_installed_version(dir, id, "1.0.0", dependency);
    }

    fn write_installed_version(dir: &Utf8Path, id: &str, version: &str, dependency: &str) {
        std::fs::create_dir_all(dir).expect("installed generator dir");
        let manifest = format!(
            r#"
[generator]
id = "{id}"
name = "{id}"
version = "{version}"
{dependency}
"#
        );
        std::fs::write(dir.join(contract::MANIFEST_FILE), &manifest).expect("installed manifest");
        // Installed registry packages always carry their supply-chain
        // metadata; the repair path verifies it before reuse (E14).
        // SBOM verification REQUIRES an explicit sanitized mode per file
        // (fail-closed, E15): record the actual sanitized mode of the
        // written manifest so the fixture verifies.
        let digest = hex::encode(Sha256::digest(manifest.as_bytes()));
        let mode = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let bits = std::fs::metadata(dir.join(contract::MANIFEST_FILE))
                    .expect("manifest metadata")
                    .permissions()
                    .mode()
                    & 0o777;
                (bits & !0o002) as u64
            }
            #[cfg(not(unix))]
            {
                0o644_u64
            }
        };
        std::fs::write(
            dir.join("SBOM.spdx.json"),
            serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [{ "fileName": contract::MANIFEST_FILE, "checksums": [{ "checksumValue": digest }], "mode": mode }],
            })
            .to_string(),
        )
        .expect("installed sbom");
        std::fs::write(
            dir.join("PROVENANCE.intoto.json"),
            r#"{"_type":"https://in-toto.io/Statement/v1"}"#,
        )
        .expect("installed provenance");
    }

    fn assert_verified_package(dir: &Utf8Path, id: &str, version: &str) {
        let limits = service::PackageLimits::default();
        service::package::verify_installed_package(dir, &limits).expect("valid inventory");
        let loaded = crate::cli::install_stage::load_verified_contract(dir, &limits)
            .expect("verified contract");
        assert_eq!(loaded.manifest.generator.id, id);
        assert_eq!(loaded.manifest.generator.version, version);
        assert!(!dir.join(".backup.json").exists());
        for entry in std::fs::read_dir(dir).expect("package files") {
            assert!(
                !entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".install-backup-")
            );
        }
    }

    fn recover_test_target(root: &Utf8Path, id: &str) -> Result<()> {
        let _global = install_commit::global_commit_rwlock()
            .read()
            .expect("global");
        let _global_file = install_commit::acquire_file_lock_shared(
            &install_commit::global_lock_path(root),
            root,
        )?;
        let _shard = install_commit::replace_mutex_for(id).lock().expect("shard");
        let _id = install_commit::acquire_file_lock(
            &root.join(install_commit::per_id_lock_name(id)),
            root,
        )?;
        install_commit::recover_interrupted_backup(
            &root.join(id),
            &service::PackageLimits::default(),
        )
    }

    /// Writes a direct directory source with supply-chain metadata (SBOM +
    /// provenance) so staging (which REQUIRES an SBOM, fail-closed E14) can
    /// verify it. The SBOM lists the manifest with its content digest + actual
    /// sanitized mode.
    /// Packable source: manifest only. `package()` generates SBOM and
    /// provenance itself and refuses pre-existing reserved metadata paths,
    /// so registry-archive fixtures must not pre-place them (E14).
    fn write_packable_source(dir: &Utf8Path, manifest: &str) {
        std::fs::create_dir_all(dir).expect("source dir should be created");
        std::fs::write(dir.join(contract::MANIFEST_FILE), manifest)
            .expect("source manifest should be written");
    }

    fn write_source_with_sbom(dir: &Utf8Path, manifest: &str) {
        std::fs::create_dir_all(dir).expect("source dir should be created");
        std::fs::write(dir.join(contract::MANIFEST_FILE), manifest)
            .expect("source manifest should be written");
        let digest = hex::encode(Sha256::digest(manifest.as_bytes()));
        let mode = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let bits = std::fs::metadata(dir.join(contract::MANIFEST_FILE))
                    .expect("manifest metadata")
                    .permissions()
                    .mode()
                    & 0o777;
                (bits & !0o002) as u64
            }
            #[cfg(not(unix))]
            {
                0o644_u64
            }
        };
        std::fs::write(
            dir.join("SBOM.spdx.json"),
            serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [{ "fileName": contract::MANIFEST_FILE, "checksums": [{ "checksumValue": digest }], "mode": mode }],
            })
            .to_string(),
        )
        .expect("source sbom should be written");
        std::fs::write(
            dir.join("PROVENANCE.intoto.json"),
            r#"{"_type":"https://in-toto.io/Statement/v1"}"#,
        )
        .expect("source provenance should be written");
    }

    #[tokio::test]
    async fn installed_parent_repairs_a_missing_dependency_closure() {
        if env_process::isolated() {
            return;
        }
        // E14: re-running an installed parent must verify (and attempt to
        // repair) its dependency closure instead of returning success.
        let root = test_root("closure");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        // An empty DATA_HOME keeps the repair attempt deterministic: with a
        // complete closure no registry is consulted, and a missing closure
        // fails on registry resolution instead of touching the network.
        let _env = set_registry_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        let config = test_config(&limits, &generators);
        install_registry_package(
            config,
            "a",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect("a complete closure must re-validate without a registry");
        std::fs::remove_dir_all(generators.join("b")).expect("remove dependency");
        let config = test_config(&limits, &generators);
        let error = install_registry_package(
            config,
            "a",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("a missing dependency must not be silently accepted");
        assert!(
            error.to_string().contains("registr"),
            "the repair must reach dependency resolution, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn installed_package_tamper_fails_inventory_verification() {
        if env_process::isolated() {
            return;
        }
        // E14: a modified installed package must fail inventory
        // verification instead of being silently reused.
        let root = test_root("tamper");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "");
        let tampered =
            std::fs::read_to_string(generators.join("b/qcg.toml")).expect("manifest readable");
        std::fs::write(
            generators.join("b/qcg.toml"),
            format!("{tampered}# tampered\n"),
        )
        .expect("tamper should write");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        let config = test_config(&limits, &generators);
        let error = install_registry_package(
            config,
            "a",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("a tampered dependency must fail closed");
        assert!(
            error.to_string().contains("inventory verification"),
            "the tamper must be reported, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn installed_parent_fails_closed_when_registry_fetch_fails() {
        if env_process::isolated() {
            return;
        }
        // E14: a configured but unreachable registry must fail the repair
        // instead of being skipped as if the closure were complete.
        let root = test_root("fetch");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config.registries.insert(
            "broken".into(),
            format!("file://{}/missing-index.toml", root.join("registry")),
        );
        crate::registry::save_registries(&home, &config).expect("registries should save");
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        let config = test_config(&limits, &generators);
        let error = install_registry_package(
            config,
            "a",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("an unreachable registry must fail the repair");
        assert!(
            error.to_string().contains("registry index"),
            "the failure must name the registry fetch, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn installed_cycle_is_rejected() {
        if env_process::isolated() {
            return;
        }
        // E14: a true A -> B -> A dependency cycle fails closed instead of
        // recursing forever or being silently accepted.
        let root = test_root("cycle");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "\n[dependencies]\na = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        let config = test_config(&limits, &generators);
        let error = install_registry_package(
            config,
            "a",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("a dependency cycle must fail closed");
        assert!(
            error.to_string().contains("dependency cycle"),
            "the cycle must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn installed_closure_accepts_a_diamond_without_a_registry() {
        if env_process::isolated() {
            return;
        }
        // E14: a diamond (A -> {B, C} -> D) must resolve without false
        // cycle detection and without consulting a registry once every
        // package in the closure is installed.
        let root = test_root("diamond");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        write_installed(
            &generators.join("a"),
            "a",
            "\n[dependencies]\nb = \"^1\"\nc = \"^1\"\n",
        );
        write_installed(&generators.join("b"), "b", "\n[dependencies]\nd = \"^1\"\n");
        write_installed(&generators.join("c"), "c", "\n[dependencies]\nd = \"^1\"\n");
        write_installed(&generators.join("d"), "d", "");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        for id in ["a", "b", "c"] {
            let config = test_config(&limits, &generators);
            install_registry_package(
                config,
                id,
                &requirement,
                &mut BTreeSet::new(),
                &mut Selected::new(),
            )
            .await
            .unwrap_or_else(|error| panic!("diamond closure `{id}` must resolve: {error}"));
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn stale_install_tmps_reap_by_cutoff_prefix_and_ownership() {
        // E14-8: install staging, archive, and backup prefixes are reaped
        // purely by age. Fresh entries survive even with an own-pid name: a
        // concurrent install in this process must not lose its live staging
        // to a sibling's sweep. A cutoff of `UNIX_EPOCH` reaps nothing,
        // proving the age gate direction without touching mtimes.
        use std::time::SystemTime;
        let root = test_root("reap");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let own_pid = std::process::id();
        let foreign_pid = own_pid.wrapping_add(1);
        let hour_ago = SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(3600))
            .expect("one hour ago must be representable");
        // Just-created entries are fresh: an hour-old cutoff reaps nothing,
        // regardless of pid ownership.
        let own_staging = root.join(format!(".install-temp-{own_pid}-old"));
        let foreign_staging = root.join(format!(".install-temp-{foreign_pid}-old"));
        let foreign_archive = root.join(format!(".install-archive-{foreign_pid}-old.pkg"));
        let foreign_backup = root.join(format!(".install-backup-{foreign_pid}-old"));
        let foreign_file = root.join("generators");
        std::fs::create_dir_all(&own_staging).expect("own staging should be created");
        std::fs::create_dir_all(&foreign_staging).expect("foreign staging should be created");
        std::fs::write(&foreign_archive, b"x").expect("foreign archive should be created");
        std::fs::create_dir_all(&foreign_backup).expect("foreign backup should be created");
        std::fs::create_dir_all(&foreign_file).expect("foreign should be created");
        reap_install_tmps_before(&root, ".install-temp-", hour_ago).expect("reap should succeed");
        assert!(
            own_staging.exists(),
            "fresh own-pid staging must survive a concurrent sweep"
        );
        assert!(
            foreign_staging.exists(),
            "fresh foreign staging must survive the age gate"
        );
        // A `now` cutoff treats just-created entries as stale: all prefixes
        // of scratch are reaped.
        reap_install_tmps_before(&root, ".install-temp-", SystemTime::now())
            .expect("reap should succeed");
        assert!(
            !own_staging.exists(),
            "stale own-pid staging must be reaped"
        );
        assert!(
            !foreign_staging.exists(),
            "old foreign staging must be reaped"
        );
        assert!(foreign_file.exists(), "a foreign dir must survive");
        // Archives are scratch; even an explicit backup prefix must retain unknown backups.
        reap_install_tmps_before(&root, ".install-archive-", SystemTime::now())
            .expect("archive reap should succeed");
        assert!(!foreign_archive.exists(), "an archive file must be reaped");
        reap_install_tmps_before(&root, ".install-backup-", SystemTime::now())
            .expect("backup reap should succeed");
        assert!(foreign_backup.exists(), "unknown backups must be retained");
        // Epoch cutoff reaps nothing foreign.
        std::fs::create_dir_all(&foreign_staging).expect("foreign staging should be recreated");
        reap_install_tmps_before(&root, ".install-temp-", SystemTime::UNIX_EPOCH)
            .expect("reap should succeed");
        assert!(
            foreign_staging.exists(),
            "nothing is older than the epoch, so nothing must be reaped"
        );
    }

    #[test]
    fn backup_with_missing_target_recovers_before_sweep() {
        if env_process::isolated() {
            return;
        }
        // F14-02: a backup whose target is missing restores instead of
        // being swept, even when the backup directory mtime looks old
        // (rename preserves it).
        let root = test_root("recover");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let backup = root.join(format!(
            ".install-backup-{}-{}",
            std::process::id().wrapping_add(777),
            uuid::Uuid::now_v7()
        ));
        write_installed(&backup, "mygen", "");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("marker");
        // Target "mygen" is missing: recovery must restore it.
        reap_stale_install_tmps(&root).expect("sweep should succeed");
        assert!(
            root.join("mygen").exists(),
            "the missing target must be restored from backup"
        );
        assert!(!backup.exists(), "recovery consumes backup");
        assert!(!install_commit::backup_sidecar_path(&backup).exists());
        assert_verified_package(&root.join("mygen"), "mygen", "1.0.0");
    }

    #[test]
    fn backup_maintenance_excludes_live_commits_and_retains_unknown_ownership() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("backup-maintenance");
        let _guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let target = root.join("mygen");
        write_installed(&target, "mygen", "");
        let backup = root.join(".install-backup-marked");
        write_installed(&backup, "mygen", "");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("marker");
        let lock_path = root.join(install_commit::per_id_lock_name("mygen"));
        let commit_guard =
            install_commit::acquire_file_lock(&lock_path, &target).expect("commit lock");
        reap_install_backup_siblings(&root, std::time::SystemTime::now()).expect("contended sweep");
        assert!(backup.exists(), "a live commit's backup must survive");
        std::fs::remove_dir_all(&target).expect("simulate commit rename window");
        recover_generators_backups(&root).expect("recovery");
        assert!(
            !target.exists(),
            "maintenance must not restore into a live commit's rename window"
        );
        assert!(backup.exists());
        drop(commit_guard);
        recover_generators_backups(&root).expect("recovery");
        assert!(
            target.exists(),
            "an abandoned commit must recover after the lock is released"
        );
        assert!(!backup.exists());

        std::fs::create_dir_all(&backup).expect("obsolete backup");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("marker");
        // Use the recorded mtime as the cutoff so filesystem timestamp
        // precision cannot make this boundary check depend on wall-clock time.
        assert_eq!(
            install_commit::backup_records(&root)
                .expect("records")
                .first()
                .map(|record| record.id.as_str()),
            Some("mygen"),
            "the obsolete backup must have valid ownership metadata"
        );
        let cutoff = install_commit::backup_freshness(
            &install_commit::backup_records(&root)
                .expect("records")
                .remove(0),
        )
        .expect("the obsolete backup must have readable freshness metadata");
        reap_install_backup_siblings(&root, cutoff).expect("uncontended sweep");
        assert!(
            !backup.exists(),
            "an owned obsolete backup should be removed"
        );
        for (name, marker) in [
            ("unmarked", None),
            ("corrupt", Some("{".to_string())),
            ("unsafe", Some(r#"{"id":"../outside"}"#.to_string())),
            (
                "oversized",
                Some(format!(
                    r#"{{"id":"mygen","extra":"{}"}}"#,
                    "x".repeat(16 * 1024)
                )),
            ),
        ] {
            let path = root.join(format!(".install-backup-{name}"));
            std::fs::create_dir_all(&path).expect("unknown backup");
            if let Some(marker) = marker {
                std::fs::write(install_commit::backup_sidecar_path(&path), marker)
                    .expect("unknown marker");
            }
            reap_install_backup_siblings(&root, std::time::SystemTime::now())
                .expect("unknown sweep");
            assert!(path.exists(), "{name} backup must be retained");
        }
    }

    #[test]
    fn uninstall_removes_owned_backups_without_resurrecting_the_generator() {
        let root = test_root("uninstall-backup");
        let _guard = TempGuard(root.clone());
        write_installed(&root.join("mygen"), "mygen", "");
        let backup = root.join(".install-backup-owned");
        write_installed(&backup, "mygen", "");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("marker");
        let unknown = root.join(".install-backup-unknown");
        std::fs::create_dir_all(&unknown).expect("unknown backup");
        uninstall("mygen", &root, true).expect("uninstall");
        recover_generators_backups(&root).expect("recovery");
        assert!(
            !root.join("mygen").exists(),
            "successful uninstall must not be undone by recovery"
        );
        assert!(!backup.exists());
        assert!(unknown.exists(), "uninstall must retain unowned data");
    }

    #[test]
    fn backup_sweep_refuses_unavailable_lock() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("backup-lock-error");
        let _guard = TempGuard(root.clone());
        write_installed(&root.join("mygen"), "mygen", "");
        let backup = root.join(".install-backup-owned");
        write_installed(&backup, "mygen", "");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("marker");
        std::fs::create_dir_all(root.join(install_commit::per_id_lock_name("mygen")))
            .expect("unopenable lock");
        reap_install_backup_siblings(&root, std::time::SystemTime::now())
            .expect_err("lock failure must refuse deletion");
        assert!(backup.exists());
    }

    #[test]
    fn fresh_marked_backup_survives_foreign_sweep() {
        // F14-01: a just-made backup keeps its old directory mtime after
        // rename; the fresh marker mtime protects it from a concurrent
        // foreign sweep while its target exists.
        let root = test_root("fresh");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        std::fs::create_dir_all(root.join("mygen")).expect("target exists");
        let backup = root.join(format!(
            ".install-backup-{}-{}",
            std::process::id().wrapping_add(778),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&backup).expect("backup dir");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("marker");
        reap_stale_install_tmps(&root).expect("sweep should succeed");
        assert!(
            backup.exists(),
            "a fresh marked backup must survive a foreign sweep"
        );
        assert!(root.join("mygen").exists(), "the target must remain");
    }

    #[test]
    fn failed_commit_restores_old_version_and_converges_on_rerun() {
        // F14-04 (commit-failure half): with an installed target, a commit
        // whose staging vanished fails; the old version is rolled back
        // intact, no orphan backup is left behind, and a rerun with fresh
        // staging converges. Fault probes below also exercise restore failures.
        let root = test_root("commitfail");
        let _temp_guard = TempGuard(root.clone());
        let target = root.join("mygen");
        write_installed(&target, "mygen", "");
        // Staging that will never exist: the commit must fail.
        let missing_staging = root.join("staging-vanished");
        let error = install_commit::commit_install(&missing_staging, &target, true)
            .expect_err("a commit without staging must fail");
        assert!(
            error.to_string().contains("failed to commit generator"),
            "the failure must name the commit: {error}"
        );
        assert_verified_package(&target, "mygen", "1.0.0");
        let orphans: Vec<_> = std::fs::read_dir(&root)
            .expect("scan")
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".install-backup-"))
            })
            .collect();
        assert!(
            orphans.is_empty(),
            "a restored rollback must consume its backup"
        );
        // Rerun with real staging converges onto the restored old version.
        let staging = root.join("staging-fresh");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        install_commit::commit_install(&staging, &target, true).expect("rerun should converge");
        assert_verified_package(&target, "mygen", "2.0.0");
    }

    #[test]
    fn corrupt_marked_backup_is_never_swept() {
        // A marker that cannot be parsed may still guard the sole copy of
        // an interrupted commit: the sweep must keep it (fail closed)
        // instead of treating it as age-gated scratch.
        let root = test_root("corrupt");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let backup = root.join(format!(
            ".install-backup-{}-{}",
            std::process::id().wrapping_add(779),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&backup).expect("backup dir");
        std::fs::write(
            install_commit::backup_sidecar_path(&backup),
            b"\x00not-json",
        )
        .expect("corrupt marker");
        // No target exists and no id parses: the sweep must still keep it.
        reap_stale_install_tmps(&root).expect("sweep should succeed");
        assert!(
            backup.exists(),
            "a corrupt-marked backup must survive the sweep"
        );
    }

    fn assert_no_backup_artifacts(root: &Utf8Path) {
        let artifacts = std::fs::read_dir(root)
            .expect("scan install parent")
            .map(|entry| entry.expect("directory entry").file_name())
            .filter(|name| {
                name.to_str()
                    .is_some_and(|name| name.starts_with(".install-backup-"))
            })
            .collect::<Vec<_>>();
        assert!(
            artifacts.is_empty(),
            "invalid replacement must not create backup paths or sidecars: {artifacts:?}"
        );
    }

    #[test]
    fn force_commit_refuses_regular_file_target_before_backup_creation() {
        let root = test_root("force-file-target");
        let _guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let target = root.join("mygen");
        std::fs::write(&target, b"existing file").expect("target file");
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging).expect("staging");

        let error = install_commit::commit_install(&staging, &target, true)
            .expect_err("--force must refuse to replace a regular file");

        assert!(error.to_string().contains("non-directory"), "{error:#}");
        assert_eq!(
            std::fs::read(&target).expect("target remains"),
            b"existing file"
        );
        assert_no_backup_artifacts(&root);
    }

    #[cfg(unix)]
    #[test]
    fn force_commit_refuses_symlink_target_without_touching_external_target() {
        use std::os::unix::fs::symlink;

        let root = test_root("force-symlink-target");
        let _guard = TempGuard(root.clone());
        let external = test_root("force-symlink-external");
        let _external_guard = TempGuard(external.clone());
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&external).expect("external parent");
        let external_target = external.join("outside");
        std::fs::write(&external_target, b"external data").expect("external target");
        let target = root.join("mygen");
        symlink(&external_target, &target).expect("target symlink");
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging).expect("staging");

        let error = install_commit::commit_install(&staging, &target, true)
            .expect_err("--force must refuse to replace a symbolic link");

        assert!(error.to_string().contains("symbolic-link"), "{error:#}");
        assert!(
            std::fs::symlink_metadata(&target)
                .expect("target link remains")
                .file_type()
                .is_symlink(),
            "the installed symlink itself must remain untouched"
        );
        assert_eq!(
            std::fs::read(&external_target).expect("external target remains"),
            b"external data",
            "the symlink target must remain untouched"
        );
        assert_no_backup_artifacts(&root);
    }

    #[test]
    fn force_commit_refuses_disappeared_target_before_backup_creation() {
        let root = test_root("force-missing-target");
        let _guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let target = root.join("mygen");
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging).expect("staging");

        let error = install_commit::commit_install(&staging, &target, true)
            .expect_err("replacement must fail when its existing target disappeared");

        assert!(error.to_string().contains("disappeared"), "{error:#}");
        assert_no_backup_artifacts(&root);
    }

    #[test]
    fn no_replace_probe_refuses_an_existing_target() {
        // E14: the non-Linux fallback must fail closed when the target is
        // observed, never silently replace it. Runs on every platform.
        let root = test_root("noreplace");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let target = root.join("a");
        std::fs::create_dir_all(&target).expect("target should be created");
        let error = check_no_replace_target(&target, false)
            .expect_err("an existing target must be refused");
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        check_no_replace_target(&target, true).expect("replace mode must pass the probe");
        check_no_replace_target(&root.join("missing"), false)
            .expect("a missing target must pass the probe");
    }

    #[tokio::test]
    async fn direct_install_verifies_inventory_and_reports_partial_closure() {
        if env_process::isolated() {
            return;
        }
        // E14: a direct (local directory) install resolves and stages the
        // FULL closure before any commit. With an unsatisfiable dependency
        // the install refuses pre-commit and never leaves a committed
        // parent behind (no rerun repair demand).
        let root = test_root("direct");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let source = root.join("source");
        std::fs::create_dir_all(&home).expect("home dir");
        std::fs::create_dir_all(source.join("sub")).expect("source dir");
        let _env = set_registry_home(&home).await;
        // Direct sources must carry an SBOM (fail-closed E14): write one so
        // the failure under test is the missing dependency, not missing
        // supply-chain metadata.
        write_source_with_sbom(
            &source.join("sub"),
            "[generator]\nid = \"direct-a\"\nname = \"Direct A\"\nversion = \"1.0.0\"\n\n[dependencies]\nmissing-dep = \"^1\"\n",
        );
        let limits = service::PackageLimits::default();
        let verification = test_verification();
        // The live source must never become the staged path: staging copies
        // into private staging.
        let live_source = source.join("sub");
        let error = install(
            None,
            live_source.as_str(),
            &generators,
            true,
            false,
            verification,
            &limits,
        )
        .await
        .expect_err("a missing dependency must fail the direct install");
        assert!(
            error.to_string().contains("cannot be satisfied")
                || error.to_string().contains("refusing to commit"),
            "the error must refuse pre-commit: {error}"
        );
        assert!(
            !generators.join("direct-a/qcg.toml").exists(),
            "a failed dependency must never leave a committed parent behind"
        );
        assert!(
            !live_source.as_str().is_empty(),
            "live source path must remain for the assertion above"
        );
    }

    /// Holds the environment lock for the test's whole lifetime: `DATA_HOME`
    /// is process-global, so any test that overrides it must not interleave
    /// with another one. Restores `DATA_HOME` on drop so a panicking assertion
    /// cannot leak the override into other tests.
    struct HomeGuard {
        _lock: tokio::sync::MutexGuard<'static, ()>,
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            // SAFETY: the guard still holds ENV_LOCK, so no other test can
            // be mutating the environment here.
            unsafe {
                std::env::remove_var("DATA_HOME");
            }
        }
    }

    fn fixture_stage(path: &Utf8Path, limits: &service::PackageLimits) -> StagedInstall {
        StagedInstall {
            path: path.to_path_buf(),
            contract: load_verified_contract(path, limits).expect("verified fixture"),
            _cleanup: CleanupPaths::default(),
        }
    }

    #[derive(Clone, Copy)]
    enum ClosureRace {
        Uninstall,
        ReplaceVersion,
        ReplaceDependencies,
    }

    async fn reused_dependency_race(race: ClosureRace) {
        let root = test_root("closure-race");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let parent = root.join("parent");
        let limits = service::PackageLimits::default();
        let config = InstallConfig {
            yes: true,
            ..test_config(&limits, &generators)
        };
        write_installed(&generators.join("dep"), "dep", "");
        // Transitive reused nodes must be retained too, not just direct deps.
        write_installed(
            &generators.join("middle"),
            "middle",
            "[dependencies]\ndep = \"*\"",
        );
        write_installed(&parent, "parent", "[dependencies]\nmiddle = \"^1\"");
        let parent_stage = fixture_stage(&parent, &limits);
        let mut selected = Selected::new();
        let mut visiting = BTreeSet::from([("parent".to_string(), "1.0.0".to_string())]);
        let stages = stage_closure_for_manifest(
            config,
            &parent_stage.contract.manifest,
            &mut visiting,
            &mut selected,
        )
        .await
        .expect("closure verified before the race");
        assert!(stages.is_empty(), "all dependencies are reused");
        assert_eq!(selected.len(), 2, "retain direct and transitive selections");
        let prepared =
            vec![install_commit::prepare_install(config, parent_stage).expect("parent prepared")];

        // Explicit pause at the real preparation/publication boundary. The
        // mutator completes a locked uninstall/replacement before publication;
        // no sleep or scheduling assumption is involved.
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let mutator = scope.spawn(|| {
                barrier.wait();
                match race {
                    ClosureRace::Uninstall => {
                        // Remove middle first because uninstall protects installed dependents.
                        uninstall("middle", &generators, true).expect("uninstall middle");
                        uninstall("dep", &generators, true).expect("uninstall selected dep");
                    }
                    ClosureRace::ReplaceVersion => {
                        let replacement = root.join("replacement");
                        // Still satisfies `*`, but it is not the selected version.
                        write_installed_version(&replacement, "dep", "2.0.0", "");
                        finish_closure(
                            InstallConfig {
                                force: true,
                                ..config
                            },
                            vec![fixture_stage(&replacement, &limits)],
                            &Selected::new(),
                        )
                        .expect("replace selected version");
                    }
                    ClosureRace::ReplaceDependencies => {
                        let replacement = root.join("replacement");
                        write_installed(&generators.join("other"), "other", "");
                        write_installed(&replacement, "dep", "[dependencies]\nother = \"^1\"");
                        let other = load_verified_contract(&generators.join("other"), &limits)
                            .expect("other");
                        let replacement_selected = Selected::from([(
                            "other".to_string(),
                            super::super::install_stage::SelectedPackage::from_manifest(
                                &other.manifest,
                            )
                            .expect("selection"),
                        )]);
                        finish_closure(
                            InstallConfig {
                                force: true,
                                ..config
                            },
                            vec![fixture_stage(&replacement, &limits)],
                            &replacement_selected,
                        )
                        .expect("replace selected dependency set");
                    }
                }
            });
            barrier.wait();
            mutator.join().expect("mutator finished");
        });
        let error = install_commit::commit_prepared_closure(config, prepared, &selected)
            .expect_err("stale selection must refuse parent publication");
        assert!(format!("{error:#}").contains("retry install"), "{error:#}");
        assert!(
            !generators.join("parent").exists(),
            "stale parent must not publish"
        );
        assert!(
            !std::fs::read_dir(&generators)
                .expect("generators")
                .flatten()
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".install-temp-")),
            "unpublished copies cleaned"
        );
    }

    #[tokio::test]
    async fn closure_commit_refuses_uninstalled_reused_dependency() {
        reused_dependency_race(ClosureRace::Uninstall).await;
    }

    #[tokio::test]
    async fn closure_commit_refuses_replaced_reused_version() {
        reused_dependency_race(ClosureRace::ReplaceVersion).await;
    }

    #[tokio::test]
    async fn closure_commit_refuses_changed_reused_dependencies() {
        reused_dependency_race(ClosureRace::ReplaceDependencies).await;
    }

    const LOCK_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    fn closure_selection(generators: &Utf8Path, limits: &service::PackageLimits) -> Selected {
        ["middle", "dep"]
            .into_iter()
            .map(|id| {
                let manifest = load_verified_contract(&generators.join(id), limits)
                    .expect("reused inventory")
                    .manifest;
                (
                    id.to_string(),
                    super::super::install_stage::SelectedPackage::from_manifest(&manifest)
                        .expect("selection"),
                )
            })
            .collect()
    }

    #[test]
    fn parallel_closure_publications_of_disjoint_ids_proceed_together() {
        // Isolate from unrelated tests' global-exclusive uninstall waiters.
        if env_process::isolated() {
            return;
        }
        let root = test_root("parallel-closures");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let limits = service::PackageLimits::default();
        let config = InstallConfig {
            yes: true,
            ..test_config(&limits, &generators)
        };
        write_installed(&root.join("leaf"), "leaf", "");
        write_installed(
            &root.join("parent"),
            "parent",
            "[dependencies]\nleaf = \"^1\"",
        );
        let other = (0..100)
            .map(|i| format!("other-{i}"))
            .find(|id| {
                ["parent", "leaf"].iter().all(|held| {
                    install_commit::replace_shard_index(id)
                        != install_commit::replace_shard_index(held)
                })
            })
            .expect("disjoint shard");
        write_installed(&root.join(&other), &other, "");
        let stages: Vec<_> = ["leaf", "parent"]
            .into_iter()
            .map(|id| fixture_stage(&root.join(id), &limits))
            .collect();
        let (paused_tx, paused_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let publisher = scope.spawn(|| {
                install_commit::with_closure_publication_hook(
                    move || {
                        paused_tx.send(()).unwrap();
                        release_rx
                            .recv_timeout(LOCK_TEST_TIMEOUT * 2)
                            .expect("release publication");
                    },
                    || finish_closure(config, stages, &Selected::new()),
                )
            });
            paused_rx
                .recv_timeout(LOCK_TEST_TIMEOUT)
                .expect("closure revalidated under locks");
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let other_id = &other;
            scope.spawn(move || {
                done_tx
                    .send(finish_closure(
                        config,
                        vec![fixture_stage(&root.join(other_id), &limits)],
                        &Selected::new(),
                    ))
                    .unwrap();
            });
            done_rx
                .recv_timeout(LOCK_TEST_TIMEOUT)
                .expect("disjoint production publication must finish while first closure is paused")
                .expect("disjoint publication");
            assert!(generators.join(&other).exists());
            assert!(!generators.join("parent").exists());
            release_tx.send(()).unwrap();
            publisher.join().unwrap().expect("first publication");
        });
        assert!(generators.join("parent").exists());
    }

    #[test]
    fn closure_publication_holds_reused_direct_and_transitive_ids() {
        if env_process::isolated() {
            return;
        }
        // Exercise both mutators against both kinds of reused selections.
        for target in ["middle", "dep"] {
            for replace in [false, true] {
                let root = test_root("protected-closure");
                let _temp_guard = TempGuard(root.clone());
                let generators = root.join("generators");
                let limits = service::PackageLimits::default();
                let config = InstallConfig {
                    yes: true,
                    ..test_config(&limits, &generators)
                };
                write_installed(&generators.join("dep"), "dep", "");
                write_installed(
                    &generators.join("middle"),
                    "middle",
                    "[dependencies]\ndep = \"^1\"",
                );
                write_installed(
                    &root.join("parent"),
                    "parent",
                    "[dependencies]\nmiddle = \"^1\"",
                );
                write_installed(&root.join("replacement"), target, "");
                let selected = closure_selection(&generators, &limits);
                let stage = fixture_stage(&root.join("parent"), &limits);
                let (paused_tx, paused_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                std::thread::scope(|scope| {
                    let publisher = scope.spawn(|| {
                        install_commit::with_closure_publication_hook(
                            move || {
                                paused_tx.send(()).unwrap();
                                release_rx
                                    .recv_timeout(LOCK_TEST_TIMEOUT * 2)
                                    .expect("release publication");
                            },
                            || finish_closure(config, vec![stage], &selected),
                        )
                    });
                    paused_rx
                        .recv_timeout(LOCK_TEST_TIMEOUT)
                        .expect("final revalidation");
                    for id in ["middle", "dep"] {
                        assert!(
                            matches!(
                                install_commit::replace_mutex_for(id).try_lock(),
                                Err(std::sync::TryLockError::WouldBlock)
                            ),
                            "reused shard protected: {id}"
                        );
                        assert!(
                            install_commit::try_acquire_file_lock(
                                &generators.join(per_id_lock_name(id))
                            )
                            .expect("probe ID lock")
                            .is_none(),
                            "reused file protected: {id}"
                        );
                    }
                    assert!(
                        matches!(
                            install_commit::global_commit_rwlock().try_write(),
                            Err(std::sync::TryLockError::WouldBlock)
                        ),
                        "uninstall excluded"
                    );
                    // The existing exact-test subprocess mechanism also makes an
                    // inexpensive check of real cross-process advisory locks.
                    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "cli::install::tests::closure_publication_file_lock_probe_child",
                        ])
                        .env("QCG_INSTALL_LOCK_PROBE", &generators)
                        .spawn()
                        .expect("lock probe child");
                    let (probe_tx, probe_rx) = std::sync::mpsc::channel();
                    scope.spawn(move || probe_tx.send(child.wait()).unwrap());
                    assert!(
                        probe_rx
                            .recv_timeout(LOCK_TEST_TIMEOUT)
                            .expect("bounded child probe")
                            .expect("child status")
                            .success()
                    );
                    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
                    let (done_tx, done_rx) = std::sync::mpsc::channel();
                    let generators = &generators;
                    let root = &root;
                    scope.spawn(move || {
                        let result = if replace {
                            let prepared = install_commit::prepare_install(
                                InstallConfig {
                                    force: true,
                                    ..config
                                },
                                fixture_stage(&root.join("replacement"), &limits),
                            )
                            .expect("replacement prepared outside locks");
                            attempt_tx.send(()).unwrap();
                            let parent = generators.join("parent");
                            install_commit::with_closure_publication_hook(
                                move || {
                                    assert!(
                                        parent.exists(),
                                        "replacement cannot revalidate before parent publication"
                                    )
                                },
                                || {
                                    install_commit::commit_prepared_closure(
                                        config,
                                        vec![prepared],
                                        &Selected::new(),
                                    )
                                },
                            )
                        } else {
                            attempt_tx.send(()).unwrap();
                            uninstall(target, generators, true)
                        };
                        done_tx.send(result).unwrap();
                    });
                    attempt_rx
                        .recv_timeout(LOCK_TEST_TIMEOUT)
                        .expect("mutator started");
                    // No timed negative assertion: the nonblocking lock probes
                    // above prove exclusion. Completion follows release only.
                    assert!(matches!(
                        done_rx.try_recv(),
                        Err(std::sync::mpsc::TryRecvError::Empty)
                    ));
                    assert!(!generators.join("parent").exists());
                    release_tx.send(()).unwrap();
                    publisher.join().unwrap().expect("parent publication");
                    let result = done_rx
                        .recv_timeout(LOCK_TEST_TIMEOUT)
                        .expect("mutator released");
                    if replace {
                        result.expect("replacement proceeds after publication");
                    } else {
                        let error = result
                            .expect_err("uninstall sees published dependent")
                            .to_string();
                        let dependent = if target == "middle" {
                            "parent"
                        } else {
                            "middle"
                        };
                        assert!(
                            error.contains("required by") && error.contains(dependent),
                            "{error}"
                        );
                    }
                });
                assert!(generators.join(target).exists());
            }
        }
    }

    #[test]
    fn closure_publication_file_lock_probe_child() {
        let Some(path) = std::env::var_os("QCG_INSTALL_LOCK_PROBE") else {
            return;
        };
        let generators = Utf8PathBuf::from_path_buf(path.into()).unwrap();
        let global = std::fs::OpenOptions::new()
            .write(true)
            .open(install_commit::global_lock_path(&generators))
            .unwrap();
        global
            .try_lock_shared()
            .expect("publication global file lock is shared");
        drop(global);
        assert!(
            install_commit::try_acquire_file_lock(&install_commit::global_lock_path(&generators))
                .unwrap()
                .is_none(),
            "uninstall's global-exclusive file lock is blocked"
        );
        for id in ["middle", "dep"] {
            assert!(
                install_commit::try_acquire_file_lock(&generators.join(per_id_lock_name(id)))
                    .unwrap()
                    .is_none(),
                "cross-process reused ID protected: {id}"
            );
        }
    }

    #[test]
    fn closure_lock_order_is_sorted_and_deduplicated() {
        if env_process::isolated() {
            return;
        }
        // Input order, duplicate IDs and shard collisions must not alter
        // either acquisition order. ID order differs from shard order here.
        let order = install_commit::closure_lock_order(["q", "b", "a", "b"]);
        assert_eq!(order.0.iter().copied().collect::<Vec<_>>(), [5, 12]);
        assert_eq!(order.1.iter().copied().collect::<Vec<_>>(), ["a", "b", "q"]);
        assert_eq!(order, install_commit::closure_lock_order(["a", "b", "q"]));

        // Exercise that very order through real closure publication too:
        // a/q collide and the prepared sequence is opposite to file-ID order.
        let root = test_root("closure-lock-order");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let limits = service::PackageLimits::default();
        let config = InstallConfig {
            yes: true,
            ..test_config(&limits, &generators)
        };
        let stages = ["q", "b", "a"]
            .into_iter()
            .map(|id| {
                write_installed(&root.join(id), id, "");
                fixture_stage(&root.join(id), &limits)
            })
            .collect();
        finish_closure(config, stages, &Selected::new()).expect("no recursive or inverted locking");
        for id in ["a", "b", "q"] {
            load_verified_contract(&generators.join(id), &limits).expect("published inventory");
        }
    }

    #[test]
    fn closure_commit_publishes_fresh_transitive_closure() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("fresh-closure");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let limits = service::PackageLimits::default();
        let config = InstallConfig {
            yes: true,
            ..test_config(&limits, &generators)
        };
        // Choose a collision to prove closure-wide locking deduplicates
        // shards even when distinct closure IDs map to the same mutex.
        let leaf = (0..100)
            .map(|i| format!("leaf-{i}"))
            .find(|id| {
                std::ptr::eq(
                    install_commit::replace_mutex_for(id),
                    install_commit::replace_mutex_for("parent"),
                )
            })
            .expect("colliding shard");
        write_installed(&root.join(&leaf), &leaf, "");
        write_installed(
            &root.join("middle"),
            "middle",
            &format!("[dependencies]\n{leaf} = \"^1\""),
        );
        write_installed(
            &root.join("parent"),
            "parent",
            "[dependencies]\nmiddle = \"^1\"",
        );
        let stages: Vec<_> = [&leaf, "middle", "parent"]
            .into_iter()
            .map(|id| fixture_stage(&root.join(id), &limits))
            .collect();
        let selected = stages
            .iter()
            .take(2)
            .map(|stage| {
                (
                    stage.contract.manifest.generator.id.clone(),
                    super::super::install_stage::SelectedPackage::from_manifest(
                        &stage.contract.manifest,
                    )
                    .expect("selection"),
                )
            })
            .collect();
        finish_closure(config, stages, &selected).expect("fresh closure publication");
        for id in [&leaf, "middle", "parent"] {
            let contract = load_verified_contract(&generators.join(id), &limits)
                .expect("installed inventory verifies");
            assert_eq!(contract.manifest.generator.id, id);
            assert_eq!(contract.manifest.generator.version, "1.0.0");
        }
        assert!(
            uninstall(&leaf, &generators, true).is_err(),
            "committed dependents protected"
        );
    }

    #[test]
    fn finish_install_force_replaces_existing_generator() {
        // E14: --force must replace an existing install; without it the
        // second install fails closed instead of silently merging.
        let root = test_root("force");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let staged = root.join("staged");
        std::fs::create_dir_all(&staged).expect("staged dir");
        write_installed(&staged, "a", "");
        let limits = service::PackageLimits::default();
        // Parsed once at stage time and threaded through commit (verified
        // load, SBOM required).
        let contract =
            load_verified_contract(&staged, &limits).expect("staged manifest should load");
        let staged_install = || StagedInstall {
            path: staged.clone(),
            contract: load_verified_contract(&staged, &limits)
                .expect("staged manifest should load"),
            _cleanup: CleanupPaths::default(),
        };
        let _ = &contract;
        finish_install(None, staged_install(), &generators, true, false, &limits)
            .expect("first install should succeed");
        let error = finish_install(None, staged_install(), &generators, true, false, &limits)
            .expect_err("second install without --force must fail");
        assert!(
            error.to_string().contains("--force"),
            "the refusal must name --force: {error}"
        );
        finish_install(None, staged_install(), &generators, true, true, &limits)
            .expect("--force must replace the existing install");
        assert!(generators.join("a/qcg.toml").exists());
        // No backup or temp residue after replace installs.
        let leftovers = std::fs::read_dir(&generators)
            .expect("generators should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                name.starts_with(".install-backup-") || name.starts_with(".install-temp-")
            })
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "transaction leftovers: {leftovers:?}");
    }

    #[tokio::test]
    async fn version_mismatch_diamond_fails_closed() {
        if env_process::isolated() {
            return;
        }
        // E14-3: a diamond requiring two different versions of D cannot
        // converge on one install dir; the second branch fails closed
        // instead of silently last-winning.
        let root = test_root("vdiamond");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        write_installed(
            &generators.join("a"),
            "a",
            "\n[dependencies]\nb = \"^1\"\nc = \"^1\"\n",
        );
        write_installed(
            &generators.join("b"),
            "b",
            "\n[dependencies]\nd = \"=1.0.0\"\n",
        );
        write_installed(
            &generators.join("c"),
            "c",
            "\n[dependencies]\nd = \"=2.0.0\"\n",
        );
        write_installed(&generators.join("d"), "d", "");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        let config = test_config(&limits, &generators);
        let error = install_registry_package(
            config,
            "a",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("a version-mismatch diamond must fail closed");
        assert!(
            error.to_string().contains("does not satisfy")
                || error.to_string().contains("conflicting requirements")
                || error.to_string().contains("registry"),
            "the mismatch must be reported, got: {error}"
        );
    }

    #[tokio::test]
    async fn direct_self_dependency_fails_closed() {
        if env_process::isolated() {
            return;
        }
        // E14-3: a generator depending on itself is a cycle and fails
        // closed, never recursing forever.
        let root = test_root("selfdep");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        write_installed(&generators.join("s"), "s", "\n[dependencies]\ns = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        let config = test_config(&limits, &generators);
        let error = install_registry_package(
            config,
            "s",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("a self-dependency must fail closed");
        // Both layers fail closed: manifest validation refuses the
        // self-edge directly, while the resolver reports a dependency
        // cycle. Accept either refusal, never success.
        let message = error.to_string();
        assert!(
            message.contains("dependency cycle") || message.contains("must not depend on itself"),
            "the self-cycle must be refused: {error}"
        );
    }

    #[tokio::test]
    async fn first_failure_then_rerun_converges_via_real_install() {
        if env_process::isolated() {
            return;
        }
        // E14 TRUE first-failure e2e (not manual fill): a direct parent
        // install with a missing dependency fails pre-commit with no parent
        // left behind; the dependency is then installed via the REAL
        // `install` path (from a directory source with SBOM, through
        // staging + commit), and rerunning the parent converges. No
        // `write_installed` manual fill is used for the dependency — both
        // installs go through `install` (stage + commit).
        let root = test_root("e2e");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let dep_src = root.join("dep-src");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_registry_home(&home).await;
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-parent\"\nname = \"E2E Parent\"\nversion = \"1.0.0\"\n\n[dependencies]\ne2e-dep = \"^1\"\n",
        );
        write_source_with_sbom(
            &dep_src,
            "[generator]\nid = \"e2e-dep\"\nname = \"E2E Dep\"\nversion = \"1.0.0\"\n",
        );
        let limits = service::PackageLimits::default();
        let verification = test_verification();
        // First attempt fails: missing dependency, no parent committed.
        let error = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            verification,
            &limits,
        )
        .await
        .expect_err("first install with missing dep must fail");
        assert!(
            error.to_string().contains("cannot be satisfied")
                || error.to_string().contains("refusing to commit"),
            "first failure must refuse pre-commit: {error}"
        );
        assert!(
            !generators.join("e2e-parent/qcg.toml").exists(),
            "failed first install must leave no parent behind"
        );
        // Fill the missing dependency via the REAL install path (not manual).
        let filled = install(
            None,
            dep_src.as_str(),
            &generators,
            true,
            false,
            test_verification(),
            &limits,
        )
        .await
        .expect("dependency install via real path must succeed");
        assert_eq!(filled, "e2e-dep");
        assert!(generators.join("e2e-dep/qcg.toml").exists());
        // Rerun converges.
        let rerun = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            test_verification(),
            &limits,
        )
        .await
        .expect("rerun after real fill must converge");
        assert_eq!(rerun, "e2e-parent");
        assert!(generators.join("e2e-parent/qcg.toml").exists());
        // No temp/backup residue after convergence.
        let leftovers = std::fs::read_dir(&generators)
            .expect("generators should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                name.starts_with(".install-backup-") || name.starts_with(".install-temp-")
            })
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "e2e must leave no residue: {leftovers:?}"
        );
    }

    #[tokio::test]
    async fn post_cycle_state_converges_without_residue() {
        if env_process::isolated() {
            return;
        }
        // E14-10: after a cycle failure, retrying fails the same way without
        // wedging on residue; no backup or temp dirs linger.
        let root = test_root("postcycle");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_registry_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "\n[dependencies]\na = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = service::PackageLimits::default();
        for _ in 0..2 {
            let config = test_config(&limits, &generators);
            let error = install_registry_package(
                config,
                "a",
                &requirement,
                &mut BTreeSet::new(),
                &mut Selected::new(),
            )
            .await
            .expect_err("a cycle must keep failing closed");
            assert!(
                error.to_string().contains("dependency cycle"),
                "the cycle must be named: {error}"
            );
        }
        let leftovers = std::fs::read_dir(&generators)
            .expect("generators should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                name.starts_with(".install-backup-") || name.starts_with(".install-temp-")
            })
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "cycle retries must leave no residue: {leftovers:?}"
        );
    }

    #[test]
    fn parallel_replace_installs_serialize_without_residue() {
        // E14-10 (non-Linux-parallel-equivalent): concurrent --force commits
        // of the same id serialize in-process and leave no backup residue.
        let root = test_root("parallel");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        std::fs::create_dir_all(&generators).expect("generators dir");
        let target = generators.join("a");
        write_installed(&target, "a", "");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let barrier = barrier.clone();
                let root = root.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let staging = root.join(format!("staging-{index}"));
                    let _ = std::fs::remove_dir_all(&staging);
                    write_installed_version(&staging, "a", &format!("1.0.{index}"), "");
                    commit_install(&staging, &root.join("generators").join("a"), true)
                })
            })
            .collect();
        let mut ok = 0;
        for handle in handles {
            if handle.join().expect("worker should not panic").is_ok() {
                ok += 1;
            }
        }
        assert!(ok >= 1, "at least one parallel replace must succeed");
        assert!(
            target.join(contract::MANIFEST_FILE).exists() || target.exists(),
            "target must exist"
        );
        let leftovers = std::fs::read_dir(&generators)
            .expect("generators should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with(".install-backup-"))
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "parallel replaces must leave no backup: {leftovers:?}"
        );
    }

    #[test]
    fn uninstall_refuses_when_dependents_exist() {
        // E14-9: uninstall fails closed when another installed generator
        // depends on the target; there is no force flag.
        let root = test_root("uninstall");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "");
        let error =
            uninstall("b", &generators, true).expect_err("dependent uninstall must be refused");
        assert!(
            error.to_string().contains("required by"),
            "the refusal must name dependents: {error}"
        );
        assert!(
            generators.join("b/qcg.toml").exists(),
            "the dependency must stay installed"
        );
        uninstall("a", &generators, true).expect("leaf uninstall should succeed");
        uninstall("b", &generators, true)
            .expect("uninstall after dependent removal should succeed");
        assert!(!generators.join("b").exists(), "the target must be removed");
    }

    #[cfg(unix)]
    #[test]
    fn uninstall_refuses_through_symlinks() {
        // E14: removing through a planted symlink would delete outside the
        // generators dir. The outside tree must survive untouched.
        let root = test_root("unlink");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let outside = root.join("outside");
        std::fs::create_dir_all(&generators).expect("generators dir");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::fs::write(outside.join(contract::MANIFEST_FILE), "marker").expect("outside marker");
        std::fs::write(outside.join("keep.txt"), "keep").expect("outside content");
        std::os::unix::fs::symlink(&outside, generators.join("linked")).expect("plant should link");
        let error =
            uninstall("linked", &generators, true).expect_err("symlink uninstall must be refused");
        assert!(
            error.to_string().contains("symbolic link"),
            "the refusal must name the cause: {error}"
        );
        assert!(
            outside.join("keep.txt").exists(),
            "the outside tree must survive untouched"
        );
    }

    #[tokio::test]
    async fn staged_dir_source_is_a_private_copy_never_live() {
        if env_process::isolated() {
            return;
        }
        // Gap 9: a directory source must be copied into private staging;
        // the staged path is never the live path, so later live mutations
        // cannot redirect the commit.
        let root = test_root("stage-copy");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_registry_home(&home).await;
        let live = root.join("live");
        write_source_with_sbom(
            &live,
            "[generator]\nid = \"live-a\"\nname = \"Live A\"\nversion = \"1.0.0\"\n",
        );
        let limits = service::PackageLimits::default();
        let staged = stage_install_source(live.as_str(), None, None, &limits)
            .await
            .expect("dir source should stage");
        assert_ne!(
            staged.path.as_str(),
            live.as_str(),
            "staged path must not be the live path"
        );
        assert!(
            staged.path.as_str().contains(".install-private-")
                || staged.path.as_str().contains(".install-stage-"),
            "staged path must live in private staging, got {}",
            staged.path
        );
        assert_eq!(
            staged.contract.manifest.generator.id, "live-a",
            "staged contract must match the live manifest"
        );
        // Mutating the live source after staging must not affect the copy.
        // NOTE: `evil.txt` is written to the LIVE source only; the staged
        // copy was already verified (SBOM inventory) before this mutation,
        // so the staged contract stays valid.
        std::fs::write(live.join("evil.txt"), b"evil").expect("live mutation should write");
        assert!(
            !staged.path.join("evil.txt").exists(),
            "the staged copy must not observe later live mutations"
        );
    }

    #[tokio::test]
    async fn registry_fetch_failure_then_rerun_repairs_via_registry() {
        if env_process::isolated() {
            return;
        }
        // E14: a dependency whose archive fetch fails first leaves no parent
        // behind; once the recording file:// registry serves the archive, a
        // rerun repairs the closure through the real registry path (no manual
        // fill). Both attempts go through `install`.
        let root = test_root("regretry");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let dep_src = root.join("dep-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_registry_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        write_packable_source(
            &dep_src,
            "[generator]\nid = \"e2e-reg-dep\"\nname = \"E2E Reg Dep\"\nversion = \"1.0.0\"\n",
        );
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-reg-parent\"\nname = \"E2E Reg Parent\"\nversion = \"1.0.0\"\n\n[dependencies]\ne2e-reg-dep = \"^1\"\n",
        );
        let archive = registry_dir.join("e2e-reg-dep.pkg");
        // First: the index names an archive that cannot be fetched.
        let url = file_url(&archive);
        std::fs::write(
            registry_dir.join("index.toml"),
            format!(
                "[[packages]]\nid = \"e2e-reg-dep\"\nversion = \"1.0.0\"\nurl = \"{url}\"\nsha256 = \"{}\"\n",
                "0".repeat(64),
            ),
        )
        .expect("index should write");
        let limits = service::PackageLimits::default();
        let verification = test_verification();
        let error = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            verification,
            &limits,
        )
        .await
        .expect_err("first install with unfetchable dep must fail");
        assert!(
            !generators.join("e2e-reg-parent/qcg.toml").exists(),
            "failed first install must leave no parent behind: {error}"
        );
        // Repair the recording registry: pack the real archive and publish
        // its digest in the index.
        crate::cli::package_cmd::package(&dep_src, &archive, &limits).expect("dep should pack");
        let digest = hex::encode(Sha256::digest(
            std::fs::read(&archive).expect("archive should read"),
        ));
        let url = file_url(&archive);
        std::fs::write(
            registry_dir.join("index.toml"),
            format!(
                "[[packages]]\nid = \"e2e-reg-dep\"\nversion = \"1.0.0\"\nurl = \"{url}\"\nsha256 = \"{digest}\"\n",
            ),
        )
        .expect("index should rewrite");
        // Rerun converges through the registry: both dep and parent land.
        let rerun = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            test_verification(),
            &limits,
        )
        .await
        .expect("rerun after registry repair must converge");
        assert_eq!(rerun, "e2e-reg-parent");
        assert!(generators.join("e2e-reg-dep/qcg.toml").exists());
        assert!(generators.join("e2e-reg-parent/qcg.toml").exists());
    }

    #[tokio::test]
    async fn dependency_preparation_failure_commits_nothing_and_rerun_converges() {
        if env_process::isolated() {
            return;
        }
        // All destination copies and confirmations finish before publication.
        // A preparation failure commits nothing; removing the blocker and
        // rerunning converges the full closure.
        let root = test_root("commitseq");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let dep_a_src = root.join("dep-a-src");
        let dep_b_src = root.join("dep-b-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_registry_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        let limits = service::PackageLimits::default();
        let mut index = String::new();
        for (src, id) in [(&dep_a_src, "e2e-c-a"), (&dep_b_src, "e2e-c-b")] {
            write_packable_source(
                src,
                &format!("[generator]\nid = \"{id}\"\nname = \"{id}\"\nversion = \"1.0.0\"\n"),
            );
            let archive = registry_dir.join(format!("{id}.pkg"));
            crate::cli::package_cmd::package(src, &archive, &limits).expect("dep should pack");
            let digest = hex::encode(Sha256::digest(
                std::fs::read(&archive).expect("archive should read"),
            ));
            index.push_str(&format!(
                "[[packages]]\nid = \"{id}\"\nversion = \"1.0.0\"\nurl = \"{url}\"\nsha256 = \"{digest}\"\n",
                url = file_url(&archive),
            ));
        }
        std::fs::write(registry_dir.join("index.toml"), index).expect("index should write");
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-c-parent\"\nname = \"E2E C Parent\"\nversion = \"1.0.0\"\n\n[dependencies]\ne2e-c-a = \"^1\"\ne2e-c-b = \"^1\"\n",
        );
        // The blocker is planted before the first run: an empty directory at
        // dep-b's target reads as not-installed during staging (no qcg.toml)
        // but refuses preparation as already-existing, before any commit.
        std::fs::create_dir_all(generators.join("e2e-c-b")).expect("blocker should create");
        let error = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            test_verification(),
            &limits,
        )
        .await
        .expect_err("dep-b preparation must fail on the blocker");
        assert!(
            !generators.join("e2e-c-a/qcg.toml").exists(),
            "no dependency should commit before preparation finishes: {error}"
        );
        assert!(
            !generators.join("e2e-c-parent/qcg.toml").exists(),
            "the parent must be refused while dep-b cannot commit: {error}"
        );
        // Remove the blocker and rerun: the full closure converges.
        std::fs::remove_dir_all(generators.join("e2e-c-b")).expect("blocker should remove");
        let rerun = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            test_verification(),
            &limits,
        )
        .await
        .expect("rerun after blocker removal must converge");
        assert_eq!(rerun, "e2e-c-parent");
        assert!(generators.join("e2e-c-b/qcg.toml").exists());
        assert!(generators.join("e2e-c-parent/qcg.toml").exists());
    }

    #[tokio::test]
    async fn registry_missing_package_leaves_no_parent_behind() {
        if env_process::isolated() {
            return;
        }
        // Gap 7: resolution happens before any commit, so an unresolvable
        // fresh package never leaves a parent directory behind.
        let root = test_root("noparent");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_registry_home(&home).await;
        let limits = service::PackageLimits::default();
        let config = InstallConfig {
            yes: true,
            ..test_config(&limits, &generators)
        };
        let requirement = semver::VersionReq::parse("^1").expect("requirement should parse");
        let error = install_registry_package(
            config,
            "missing-pkg",
            &requirement,
            &mut BTreeSet::new(),
            &mut Selected::new(),
        )
        .await
        .expect_err("missing registry package must fail");
        assert!(
            !generators.join("missing-pkg").exists(),
            "no parent may be left behind, got error: {error}"
        );
    }

    /// File-backed test registry: packs sources and appends index
    /// records so registry-path installs run fully offline.
    struct RegistryPublisher<'a> {
        registry_dir: &'a Utf8Path,
        index: String,
        root: &'a Utf8Path,
        limits: &'a service::PackageLimits,
    }

    impl RegistryPublisher<'_> {
        /// Packs `src_name` (whose manifest states `version`) under a
        /// unique archive name and appends its index record.
        fn publish(&mut self, src_name: &str, id: &str, version: &str, deps: &str) {
            let src = self.root.join(src_name);
            let manifest = if deps.is_empty() {
                format!("[generator]\nid = \"{id}\"\nname = \"{id}\"\nversion = \"{version}\"\n")
            } else {
                format!(
                    "[generator]\nid = \"{id}\"\nname = \"{id}\"\nversion = \"{version}\"\n\n[dependencies]{deps}"
                )
            };
            write_packable_source(&src, &manifest);
            let archive = self.registry_dir.join(format!("{src_name}.pkg"));
            crate::cli::package_cmd::package(&src, &archive, self.limits).expect("dep should pack");
            let digest = hex::encode(Sha256::digest(
                std::fs::read(&archive).expect("archive should read"),
            ));
            self.index.push_str(&format!(
                "[[packages]]\nid = \"{id}\"\nversion = \"{version}\"\nurl = \"{url}\"\nsha256 = \"{digest}\"\n",
                url = file_url(&archive),
            ));
        }
    }

    #[tokio::test]
    async fn registry_diamond_stages_shared_dependency_once() {
        if env_process::isolated() {
            return;
        }
        // Closure-wide single selection: a satisfiable diamond (parent ->
        // {a, b} -> shared) stages and commits shared exactly once without
        // --force, selecting the highest satisfying version.
        let root = test_root("diamond-once");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_registry_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        let limits = service::PackageLimits::default();
        let mut publisher = RegistryPublisher {
            registry_dir: &registry_dir,
            index: String::new(),
            root: &root,
            limits: &limits,
        };
        for (src_name, id, version, deps) in [
            ("a-src", "e2e-d-a", "1.0.0", "\ne2e-d-shared = \"^1\"\n"),
            ("b-src", "e2e-d-b", "1.0.0", "\ne2e-d-shared = \"^1\"\n"),
            ("shared-100-src", "e2e-d-shared", "1.0.0", ""),
            ("shared-150-src", "e2e-d-shared", "1.5.0", ""),
        ] {
            publisher.publish(src_name, id, version, deps);
        }
        std::fs::write(registry_dir.join("index.toml"), publisher.index)
            .expect("index should write");
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-d-parent\"\nname = \"E2E D Parent\"\nversion = \"1.0.0\"\n\n[dependencies]\ne2e-d-a = \"^1\"\ne2e-d-b = \"^1\"\n",
        );
        let installed = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            test_verification(),
            &limits,
        )
        .await
        .expect("satisfiable diamond must install without force");
        assert_eq!(installed, "e2e-d-parent");
        for id in ["e2e-d-a", "e2e-d-b", "e2e-d-parent", "e2e-d-shared"] {
            assert!(
                generators.join(format!("{id}/qcg.toml")).exists(),
                "diamond member `{id}` must be installed"
            );
        }
        let shared = std::fs::read_to_string(generators.join("e2e-d-shared/qcg.toml"))
            .expect("shared manifest should read");
        assert!(
            shared.contains("version = \"1.5.0\""),
            "shared must resolve once to the highest satisfying version, got: {shared}"
        );
    }

    #[tokio::test]
    async fn registry_conflicting_diamond_refuses_even_with_force() {
        if env_process::isolated() {
            return;
        }
        // Incompatible sibling requirements (a -> shared=1.0.0,
        // b -> shared=2.0.0) refuse pre-commit: no stage may commit, so
        // --force cannot paper over the conflict with a last-wins replace.
        let root = test_root("diamond-conflict");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_registry_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        let limits = service::PackageLimits::default();
        let mut publisher = RegistryPublisher {
            registry_dir: &registry_dir,
            index: String::new(),
            root: &root,
            limits: &limits,
        };
        for (src_name, id, version, deps) in [
            ("a-src", "e2e-x-a", "1.0.0", "\ne2e-x-shared = \"=1.0.0\"\n"),
            ("b-src", "e2e-x-b", "1.0.0", "\ne2e-x-shared = \"=2.0.0\"\n"),
            ("shared-100-src", "e2e-x-shared", "1.0.0", ""),
            ("shared-200-src", "e2e-x-shared", "2.0.0", ""),
        ] {
            publisher.publish(src_name, id, version, deps);
        }
        std::fs::write(registry_dir.join("index.toml"), publisher.index)
            .expect("index should write");
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-x-parent\"\nname = \"E2E X Parent\"\nversion = \"1.0.0\"\n\n[dependencies]\ne2e-x-a = \"^1\"\ne2e-x-b = \"^1\"\n",
        );
        let error = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            true,
            test_verification(),
            &limits,
        )
        .await
        .expect_err("conflicting diamond must refuse even with force");
        assert!(
            error.to_string().contains("conflicting requirements"),
            "the conflict must be named, got: {error}"
        );
        for id in ["e2e-x-parent", "e2e-x-shared", "e2e-x-a", "e2e-x-b"] {
            assert!(
                !generators.join(format!("{id}/qcg.toml")).exists(),
                "conflict must commit nothing, but `{id}` exists: {error}"
            );
        }
    }

    #[tokio::test]
    async fn installed_reuse_conflicts_with_forced_replacement() {
        if env_process::isolated() {
            return;
        }
        // A verified installed reuse recorded earlier in the plan cannot be
        // invalidated by a later forced replacement: shared@1.0.0 satisfies
        // a's ^1, so b's ^2 conflicts pre-commit and the installed 1.0.0
        // stays put.
        let root = test_root("reuse-conflict");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_registry_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        write_installed(&generators.join("e2e-r-shared"), "e2e-r-shared", "");
        let limits = service::PackageLimits::default();
        let mut publisher = RegistryPublisher {
            registry_dir: &registry_dir,
            index: String::new(),
            root: &root,
            limits: &limits,
        };
        for (src_name, id, version, deps) in [
            ("a-src", "e2e-r-a", "1.0.0", "\ne2e-r-shared = \"^1\"\n"),
            ("b-src", "e2e-r-b", "1.0.0", "\ne2e-r-shared = \"^2\"\n"),
            ("shared-200-src", "e2e-r-shared", "2.0.0", ""),
        ] {
            publisher.publish(src_name, id, version, deps);
        }
        std::fs::write(registry_dir.join("index.toml"), publisher.index)
            .expect("index should write");
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-r-parent\"\nname = \"E2E R Parent\"\nversion = \"1.0.0\"\n\n[dependencies]\ne2e-r-a = \"^1\"\ne2e-r-b = \"^1\"\n",
        );
        let error = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            true,
            test_verification(),
            &limits,
        )
        .await
        .expect_err("forced replacement of a reused install must refuse");
        assert!(
            error.to_string().contains("conflicting requirements"),
            "the conflict must be named, got: {error}"
        );
        let shared = std::fs::read_to_string(generators.join("e2e-r-shared/qcg.toml"))
            .expect("reused install must stay");
        assert!(
            shared.contains("version = \"1.0.0\""),
            "installed shared must stay at 1.0.0, got: {shared}"
        );
        assert!(
            !generators.join("e2e-r-parent/qcg.toml").exists(),
            "conflict must commit no parent: {error}"
        );
    }

    #[test]
    fn concurrent_uninstalls_of_same_id_serialize_atomically() {
        // Gap 11: the dependents re-check plus removal holds the install
        // locks, so concurrent uninstalls of the same id serialize instead
        // of racing; exactly one succeeds and no residue remains.
        let root = test_root("uninstall-race");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        write_installed(&generators.join("x"), "x", "");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let barrier = barrier.clone();
                let generators = generators.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    uninstall("x", &generators, true)
                })
            })
            .collect();
        let mut ok = 0;
        for handle in handles {
            if handle.join().expect("worker should not panic").is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 1, "exactly one concurrent uninstall must succeed");
        assert!(!generators.join("x").exists(), "the target must be removed");
    }

    #[test]
    fn uninstall_refuses_when_dependent_is_tampered() {
        // Gap 12: the dependents scan uses verified loads, so a tampered
        // dependent fails closed instead of silently permitting removal.
        let root = test_root("tamper-dep");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "");
        let tampered =
            std::fs::read_to_string(generators.join("a/qcg.toml")).expect("manifest readable");
        std::fs::write(
            generators.join("a/qcg.toml"),
            format!("{tampered}# tampered\n"),
        )
        .expect("tamper should write");
        let error =
            uninstall("b", &generators, true).expect_err("tampered dependent must fail closed");
        assert!(
            error.to_string().contains("inventory")
                || error.to_string().contains("verification")
                || error.to_string().contains("failed to inspect"),
            "the tamper must be reported, got: {error}"
        );
        assert!(
            generators.join("b/qcg.toml").exists(),
            "the dependency must stay installed after a failed closed refusal"
        );
    }

    #[test]
    fn parallel_installs_of_different_ids_proceed_together() {
        // Gap 13: per-id locks let unrelated ids commit in parallel; all
        // must succeed with no backup residue.
        let root = test_root("shard");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        std::fs::create_dir_all(&generators).expect("generators dir should be created");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let barrier = barrier.clone();
                let root = root.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let id = format!("shard-{index}");
                    let staging = root.join(format!("staging-{index}"));
                    let _ = std::fs::remove_dir_all(&staging);
                    std::fs::create_dir_all(&staging).expect("staging dir should be created");
                    std::fs::write(staging.join("data.txt"), b"data").expect("staging file");
                    commit_install(&staging, &root.join("generators").join(&id), false)
                })
            })
            .collect();
        let mut ok = 0;
        for handle in handles {
            if handle.join().expect("worker should not panic").is_ok() {
                ok += 1;
            }
        }
        assert_eq!(ok, 8, "all parallel installs of distinct ids must succeed");
        for index in 0..8 {
            assert!(
                generators.join(format!("shard-{index}/data.txt")).exists(),
                "distinct target {index} must exist"
            );
        }
        let leftovers = std::fs::read_dir(&generators)
            .expect("generators should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with(".install-backup-"))
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "parallel distinct commits must leave no backup: {leftovers:?}"
        );
    }

    #[test]
    fn per_id_lock_names_are_safe_and_distinct() {
        // Gap 13: lock file names never carry path separators and distinct
        // ids never share a lock file.
        let first = per_id_lock_name("a/b");
        let second = per_id_lock_name("a_b");
        assert!(
            !first.contains('/') && !first.contains('\\') && !first.contains('\0'),
            "lock name must be safe: {first}"
        );
        assert_ne!(
            first, second,
            "sanitization collisions must be disambiguated by hash"
        );
        assert!(first.starts_with(".install-") && first.ends_with(".lock"));
    }

    #[test]
    fn backup_prepared_collision_survives_retry_maintenance_and_uninstall() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("backup-prepared-collision");
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        let staging = root.join("staging");
        write_installed(&target, "mygen", "");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        let observed_root = root.clone();
        let sentinel = b"unrelated directory\0must survive\xff";
        let error = install_commit::with_backup_probe(
            move |point| {
                if point == "marker-durable" {
                    let records = install_commit::backup_records(&observed_root)?;
                    assert_eq!(records.len(), 1);
                    std::fs::create_dir(&records[0].backup)?;
                    std::fs::write(records[0].backup.join("sentinel"), sentinel)?;
                }
                Ok(())
            },
            || install_commit::commit_install(&staging, &target, true),
        )
        .expect_err("no-replace must reject the planted backup directory");
        assert!(format!("{error:#}").contains("failed to move existing generator"));
        let records = install_commit::backup_records(&root).expect("records");
        assert_eq!(records.len(), 1);
        let record = &records[0];
        let metadata = std::fs::read(&record.sidecar).expect("prepared metadata");
        install_commit::commit_install(&staging, &target, true).expect_err("ambiguous retry");
        reap_install_backup_siblings(&root, std::time::SystemTime::now())
            .expect_err("age and verified target cannot authorize deleting preparation");
        uninstall("mygen", &root, true).expect_err("ambiguous uninstall");
        assert_verified_package(&target, "mygen", "1.0.0");
        assert_verified_package(&staging, "mygen", "2.0.0");
        assert_eq!(
            std::fs::read(record.backup.join("sentinel")).unwrap(),
            sentinel
        );
        assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);

        // Preparation cannot restore either, even if the colliding directory
        // happens to be a fully verified package with the expected identity.
        std::fs::remove_file(record.backup.join("sentinel")).expect("replace collision fixture");
        write_installed(&record.backup, "mygen", "");
        std::fs::remove_dir_all(&target).expect("simulate missing target");
        recover_test_target(&root, "mygen").expect_err("preparation is not ownership");
        recover_generators_backups(&root).expect_err("maintenance refuses adoption");
        assert!(!target.exists());
        assert_verified_package(&record.backup, "mygen", "1.0.0");
        assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
    }

    #[test]
    fn backup_crash_probes_cover_durable_sidecar_before_and_after_rename() {
        if env_process::isolated() {
            return;
        }
        for crash in [
            "marker-durable",
            "backup-renamed",
            "backup-sync",
            "ownership-write",
            "backup-owned",
        ] {
            let root = test_root(crash);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let staging = root.join("staging");
            write_installed(&target, "mygen", "");
            write_installed_version(&staging, "mygen", "2.0.0", "");
            let observed_root = root.clone();
            let mut phases = Vec::new();
            install_commit::with_backup_probe(
                move |point| {
                    phases.push(point.to_string());
                    if point == crash {
                        assert_eq!(
                            &phases[..3],
                            &[
                                "marker-file-synced",
                                "marker-parent-synced",
                                "marker-durable"
                            ]
                        );
                        let records = install_commit::backup_records(&observed_root)?;
                        assert_eq!(records.len(), 1);
                        let record = &records[0];
                        assert!(record.sidecar.is_file());
                        assert_eq!(record.backup.exists(), crash != "marker-durable");
                        assert_eq!(
                            observed_root.join("mygen").exists(),
                            crash == "marker-durable"
                        );
                        anyhow::bail!("simulated crash at {crash}");
                    }
                    Ok(())
                },
                || install_commit::commit_install(&staging, &target, true),
            )
            .expect_err("crash leaves recovery state without rollback or drop cleanup");
            let records = install_commit::backup_records(&root).expect("records");
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.backup.exists(), crash != "marker-durable");
            if matches!(crash, "backup-renamed" | "backup-sync" | "ownership-write") {
                let metadata = std::fs::read(&record.sidecar).expect("metadata");
                let error = recover_test_target(&root, "mygen").expect_err("ambiguous rename");
                assert!(format!("{error:#}").contains("preparation does not prove ownership"));
                recover_generators_backups(&root).expect_err("ambiguous maintenance");
                install_commit::commit_install(&staging, &target, true)
                    .expect_err("retry must not adopt a prepared directory");
                assert!(!target.exists());
                assert_verified_package(&record.backup, "mygen", "1.0.0");
                assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
                // An operator can validate and explicitly resolve this state;
                // automatic recovery must never infer which rename occurred.
                std::fs::rename(&record.backup, &target).expect("operator restores");
                std::fs::remove_file(&record.sidecar).expect("operator resolves record");
            } else {
                recover_test_target(&root, "mygen").expect("recover owned or absent backup");
            }
            assert_verified_package(&target, "mygen", "1.0.0");
            // A missing prepared backup permits sidecar-only cleanup under the
            // verified target; a durably owned backup permits verified restore.
            install_commit::commit_install(&staging, &target, true).expect("retry converges");
            assert_verified_package(&target, "mygen", "2.0.0");
            assert!(
                install_commit::backup_records(&root)
                    .expect("records")
                    .is_empty()
            );
        }
    }

    #[test]
    fn backup_marker_sync_errors_never_rename_the_target() {
        for failure in ["marker-file-synced", "marker-parent-synced"] {
            let root = test_root(failure);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let staging = root.join("staging");
            write_installed(&target, "mygen", "");
            write_installed_version(&staging, "mygen", "2.0.0", "");
            install_commit::with_backup_probe(
                move |point| {
                    anyhow::ensure!(point != failure, "injected marker sync error");
                    Ok(())
                },
                || install_commit::commit_install(&staging, &target, true),
            )
            .expect_err("sync failure");
            assert_verified_package(&target, "mygen", "1.0.0");
            let records = install_commit::backup_records(&root).expect("records");
            assert_eq!(records.len(), 1);
            assert!(!records[0].backup.exists());
            install_commit::commit_install(&staging, &target, true).expect("retry cleans orphan");
            assert_verified_package(&target, "mygen", "2.0.0");
        }
    }

    #[test]
    fn backup_publish_failure_restores_verified_old_package() {
        let failure = "publish-rename";
        let root = test_root(failure);
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        let staging = root.join("staging");
        write_installed(&target, "mygen", "");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        install_commit::with_backup_probe(
            move |point| {
                anyhow::ensure!(point != failure, "injected {failure}");
                Ok(())
            },
            || install_commit::commit_install(&staging, &target, true),
        )
        .expect_err("failed publication");
        assert_verified_package(&target, "mygen", "1.0.0");
        assert!(staging.exists());
        assert!(
            install_commit::backup_records(&root)
                .expect("records")
                .is_empty()
        );
    }

    #[test]
    fn backup_publish_sync_failure_preserves_published_target_and_recovery_pair() {
        let root = test_root("publish-sync");
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        let staging = root.join("staging");
        write_installed(&target, "mygen", "");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        install_commit::with_backup_probe(
            |point| {
                anyhow::ensure!(
                    point != "publish-sync",
                    "injected fsync failure after successful rename"
                );
                Ok(())
            },
            || install_commit::commit_install(&staging, &target, true),
        )
        .expect_err("uncertain durability");
        assert_verified_package(&target, "mygen", "2.0.0");
        let records = install_commit::backup_records(&root).expect("records");
        assert_eq!(records.len(), 1);
        assert_verified_package(&records[0].backup, "mygen", "1.0.0");
        // A tampered current target cannot authorize cleanup or another generation.
        std::fs::write(target.join("extra.txt"), "not in inventory").expect("tamper");
        let next = root.join("next");
        write_installed_version(&next, "mygen", "3.0.0", "");
        let error =
            install_commit::commit_install(&next, &target, true).expect_err("pending recovery");
        assert!(format!("{error:#}").contains("cleanup pending"));
        assert!(records[0].backup.exists());
        assert_eq!(
            install_commit::backup_records(&root)
                .expect("records")
                .len(),
            1
        );
        std::fs::remove_file(target.join("extra.txt")).expect("repair target");
        install_commit::commit_install(&next, &target, true)
            .expect("verify and sync before cleanup/replacement");
        assert_verified_package(&target, "mygen", "3.0.0");
        assert!(
            install_commit::backup_records(&root)
                .expect("records")
                .is_empty()
        );
    }

    #[test]
    fn backup_restore_phase_write_and_sync_failures_retain_recovery_metadata() {
        for failure in ["restoring-write", "restore-sync"] {
            let root = test_root(failure);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            write_installed(&target, "mygen", "");
            let missing = root.join("missing-staging");
            let error = install_commit::with_backup_probe(
                move |point| {
                    anyhow::ensure!(point != failure, "injected {failure}");
                    Ok(())
                },
                || install_commit::commit_install(&missing, &target, true),
            )
            .expect_err("rollback failure");
            assert!(format!("{error:#}").contains("failed to restore"));
            let records = install_commit::backup_records(&root).expect("records");
            assert_eq!(records.len(), 1);
            assert!(records[0].sidecar.exists());
            assert_eq!(records[0].backup.exists(), failure == "restoring-write");
            assert_eq!(target.exists(), failure == "restore-sync");
            recover_test_target(&root, "mygen").expect("retry recovery");
            assert_verified_package(&target, "mygen", "1.0.0");
            let next = root.join("next");
            write_installed_version(&next, "mygen", "2.0.0", "");
            install_commit::commit_install(&next, &target, true)
                .expect("clean orphan record after verified target");
            assert!(
                install_commit::backup_records(&root)
                    .expect("records")
                    .is_empty()
            );
        }
    }

    #[test]
    fn backup_restoring_collision_survives_retry_maintenance_and_uninstall() {
        if env_process::isolated() {
            return;
        }
        for interruption in ["restore-sync", "cleanup-sidecar"] {
            let root = test_root(interruption);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let backup = root.join(".install-backup-restored");
            write_installed(&backup, "mygen", "");
            let record = install_commit::write_owned_backup_marker(&backup, "mygen")
                .expect("owned backup record");
            install_commit::with_backup_probe(
                move |point| {
                    anyhow::ensure!(point != interruption, "interrupted at {interruption}");
                    Ok(())
                },
                || recover_test_target(&root, "mygen"),
            )
            .expect_err("interruption after successful restore rename");
            assert_verified_package(&target, "mygen", "1.0.0");
            assert!(!backup.exists());
            let metadata = std::fs::read(&record.sidecar).expect("restoring metadata");
            let data: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
            assert_eq!(data["phase"], "restoring");

            // A different package with the same ID still does not belong to
            // this record. Neither verification nor age can adopt it.
            write_installed_version(&backup, "mygen", "9.0.0", "");
            let sentinel = b"unrelated directory\0must survive\xff";
            std::fs::write(backup.join("sentinel"), sentinel).expect("sentinel");
            let staging = root.join("staging");
            write_installed_version(&staging, "mygen", "2.0.0", "");
            install_commit::commit_install(&staging, &target, true)
                .expect_err("retry must not delete a restoring collision");
            reap_install_backup_siblings(&root, std::time::SystemTime::now())
                .expect_err("sweep must retain restoring collision");
            uninstall("mygen", &root, true)
                .expect_err("uninstall must retain collision and target");
            assert_verified_package(&target, "mygen", "1.0.0");
            assert_verified_package(&staging, "mygen", "2.0.0");
            assert_eq!(std::fs::read(backup.join("sentinel")).unwrap(), sentinel);
            assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);

            std::fs::remove_dir_all(&target).expect("simulate lost target");
            recover_test_target(&root, "mygen").expect_err("never restore a collision");
            recover_generators_backups(&root).expect_err("maintenance must not adopt collision");
            assert!(!target.exists());
            assert_eq!(std::fs::read(backup.join("sentinel")).unwrap(), sentinel);
            std::fs::remove_dir_all(&backup).expect("remove collision fixture");
            recover_test_target(&root, "mygen")
                .expect_err("missing target and backup are still ambiguous");
            assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
        }
    }

    #[test]
    fn backup_restoring_interruption_before_rename_retains_original_package() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("restoring-before-rename");
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        let backup = root.join(".install-backup-original");
        write_installed(&backup, "mygen", "");
        let record = install_commit::write_owned_backup_marker(&backup, "mygen")
            .expect("owned backup record");
        install_commit::with_backup_probe(
            |point| {
                anyhow::ensure!(point != "restore-rename", "crash before rename syscall");
                Ok(())
            },
            || recover_test_target(&root, "mygen"),
        )
        .expect_err("interrupted after durable restoring phase, before rename");
        let metadata = std::fs::read(&record.sidecar).expect("restoring metadata");
        let data: serde_json::Value = serde_json::from_slice(&metadata).unwrap();
        assert_eq!(data["phase"], "restoring");
        assert!(!target.exists());
        let staging = root.join("staging");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        recover_test_target(&root, "mygen").expect_err("ambiguous recovery");
        recover_generators_backups(&root).expect_err("ambiguous maintenance");
        install_commit::commit_install(&staging, &target, false).expect_err("ambiguous retry");
        assert!(!target.exists());
        assert_verified_package(&backup, "mygen", "1.0.0");
        // Even a verified target arriving later does not authorize deletion
        // of the original directory still at the restoring basename.
        write_installed_version(&target, "mygen", "3.0.0", "");
        reap_install_backup_siblings(&root, std::time::SystemTime::now())
            .expect_err("retain original package");
        uninstall("mygen", &root, true).expect_err("retain original and current packages");
        assert_verified_package(&backup, "mygen", "1.0.0");
        assert_verified_package(&target, "mygen", "3.0.0");
        assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
    }

    #[test]
    fn backup_restoring_orphan_retirement_requires_verified_target() {
        if env_process::isolated() {
            return;
        }
        for action in ["retry", "sweep", "uninstall"] {
            let root = test_root(action);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let backup = root.join(".install-backup-restored");
            write_installed(&backup, "mygen", "");
            let record = install_commit::write_owned_backup_marker(&backup, "mygen")
                .expect("owned backup record");
            install_commit::with_backup_probe(
                |point| {
                    anyhow::ensure!(point != "restore-sync", "interrupted after rename");
                    Ok(())
                },
                || recover_test_target(&root, "mygen"),
            )
            .expect_err("restore interrupted");
            let metadata = std::fs::read(&record.sidecar).unwrap();
            let staging = root.join("staging");
            write_installed_version(&staging, "mygen", "2.0.0", "");
            let manifest = target.join(contract::MANIFEST_FILE);
            let original = std::fs::read(&manifest).unwrap();
            std::fs::write(&manifest, b"invalid manifest").unwrap();
            let run = || match action {
                "retry" => install_commit::commit_install(&staging, &target, true),
                "sweep" => reap_install_backup_siblings(&root, std::time::SystemTime::now()),
                "uninstall" => uninstall("mygen", &root, true),
                _ => unreachable!(),
            };
            run().expect_err("unverified target cannot retire restoring sidecar");
            assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
            assert!(!backup.exists());
            std::fs::write(&manifest, original).unwrap();
            run().expect("verified target permits sidecar-only cleanup");
            assert!(!record.sidecar.exists());
            assert!(!backup.exists());
            if action == "uninstall" {
                assert!(!target.exists());
            } else {
                let version = if action == "retry" { "2.0.0" } else { "1.0.0" };
                assert_verified_package(&target, "mygen", version);
            }
        }
    }

    #[test]
    fn backup_restore_preserves_replanted_vacant_backup_path() {
        let root = test_root("restore-replanted-backup");
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        let backup = root.join(".install-backup-restored");
        write_installed(&backup, "mygen", "");
        let record = install_commit::write_owned_backup_marker(&backup, "mygen")
            .expect("owned backup record");
        let planted_backup = record.backup.clone();
        let recovery_root = root.clone();
        let sentinel = b"unrelated directory\0must survive\xff";

        install_commit::with_backup_probe(
            move |point| {
                if point == "restore-sync" {
                    std::fs::create_dir(&planted_backup)?;
                    std::fs::write(planted_backup.join("sentinel"), sentinel)?;
                }
                Ok(())
            },
            move || recover_test_target(&recovery_root, "mygen"),
        )
        .expect("recovery should restore and retire its sidecar");

        assert_verified_package(&target, "mygen", "1.0.0");
        assert_eq!(
            std::fs::read(record.backup.join("sentinel")).expect("sentinel bytes"),
            sentinel
        );
        assert!(record.backup.is_dir(), "planted directory must remain");
        assert!(
            !record.sidecar.exists(),
            "successful restoration retires only its sidecar"
        );
    }

    fn interrupt_backup_cleanup(
        root: &Utf8Path,
        interruption: &'static str,
    ) -> install_commit::BackupRecord {
        let target = root.join("mygen");
        let staging = root.join("staging");
        write_installed(&target, "mygen", "");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        install_commit::with_backup_probe(
            move |point| {
                anyhow::ensure!(point != interruption, "interrupted at {interruption}");
                Ok(())
            },
            || install_commit::commit_install(&staging, &target, true),
        )
        .expect_err("cleanup interrupted");
        assert_verified_package(&target, "mygen", "2.0.0");
        let mut records = install_commit::backup_records(root).expect("records");
        assert_eq!(records.len(), 1);
        let record = records.pop().unwrap();
        let data: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&record.sidecar).unwrap()).unwrap();
        assert_eq!(data["phase"], "cleaning");
        record
    }

    #[test]
    fn backup_cleaning_collision_survives_retry_maintenance_and_uninstall() {
        if env_process::isolated() {
            return;
        }
        for collision in [
            "directory",
            "file",
            #[cfg(unix)]
            "symlink",
        ] {
            let root = test_root(collision);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let record = interrupt_backup_cleanup(&root, "cleanup-sidecar");
            assert!(!record.backup.exists());
            let metadata = std::fs::read(&record.sidecar).unwrap();
            let sentinel = b"unrelated directory\0must survive\xff";
            let sentinel_dir = if collision == "directory" {
                record.backup.clone()
            } else {
                root.join("unrelated")
            };
            std::fs::create_dir(&sentinel_dir).unwrap();
            std::fs::write(sentinel_dir.join("sentinel"), sentinel).unwrap();
            if collision == "file" {
                std::fs::write(&record.backup, sentinel).unwrap();
            }
            #[cfg(unix)]
            if collision == "symlink" {
                std::os::unix::fs::symlink(&sentinel_dir, &record.backup).unwrap();
            }
            let staging = root.join("next");
            write_installed_version(&staging, "mygen", "3.0.0", "");
            install_commit::commit_install(&staging, &target, true)
                .expect_err("retry must retain cleaning collision");
            reap_install_backup_siblings(&root, std::time::SystemTime::now())
                .expect_err("sweep must retain cleaning collision");
            uninstall("mygen", &root, true)
                .expect_err("uninstall must retain cleaning collision and target");
            assert_verified_package(&target, "mygen", "2.0.0");
            assert_verified_package(&staging, "mygen", "3.0.0");
            assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
            assert_eq!(
                std::fs::read(sentinel_dir.join("sentinel")).unwrap(),
                sentinel
            );
            let meta = std::fs::symlink_metadata(&record.backup).expect("collision retained");
            match collision {
                "directory" => assert!(meta.is_dir()),
                "file" => assert_eq!(std::fs::read(&record.backup).unwrap(), sentinel),
                #[cfg(unix)]
                "symlink" => assert_eq!(std::fs::read_link(&record.backup).unwrap(), sentinel_dir),
                _ => unreachable!(),
            }

            std::fs::remove_dir_all(&target).unwrap();
            recover_test_target(&root, "mygen").expect_err("never restore a cleaning collision");
            recover_generators_backups(&root).expect_err("maintenance must not adopt collision");
            assert!(!target.exists());
            assert_eq!(
                std::fs::read(sentinel_dir.join("sentinel")).unwrap(),
                sentinel
            );
            if meta.is_dir() {
                std::fs::remove_dir_all(&record.backup).unwrap();
            } else {
                std::fs::remove_file(&record.backup).unwrap();
            }
            recover_test_target(&root, "mygen")
                .expect_err("missing target and backup remain ambiguous");
            assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
        }
    }

    #[test]
    fn backup_cleaning_orphan_retirement_requires_verified_target() {
        if env_process::isolated() {
            return;
        }
        for action in ["retry", "sweep", "uninstall"] {
            let root = test_root(action);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let record = interrupt_backup_cleanup(&root, "cleanup-sidecar");
            assert!(!record.backup.exists());
            let metadata = std::fs::read(&record.sidecar).unwrap();
            let staging = root.join("next");
            write_installed_version(&staging, "mygen", "3.0.0", "");
            let manifest = target.join(contract::MANIFEST_FILE);
            let original = std::fs::read(&manifest).unwrap();
            std::fs::write(&manifest, b"invalid manifest").unwrap();
            let run = || match action {
                "retry" => install_commit::commit_install(&staging, &target, true),
                "sweep" => reap_install_backup_siblings(&root, std::time::SystemTime::now()),
                "uninstall" => uninstall("mygen", &root, true),
                _ => unreachable!(),
            };
            run().expect_err("unverified target cannot retire cleaning sidecar");
            assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
            assert!(!record.backup.exists());
            std::fs::write(&manifest, original).unwrap();
            run().expect("verified target permits sidecar-only cleanup");
            assert!(!record.sidecar.exists());
            assert!(!record.backup.exists());
            if action == "uninstall" {
                assert!(!target.exists());
            } else {
                let version = if action == "retry" { "3.0.0" } else { "2.0.0" };
                assert_verified_package(&target, "mygen", version);
            }
        }
    }

    #[test]
    fn backup_cleaning_interruption_before_removal_retains_original_package() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("cleaning-before-removal");
        let _guard = TempGuard(root.clone());
        let record = interrupt_backup_cleanup(&root, "cleaning-durable");
        let metadata = std::fs::read(&record.sidecar).unwrap();
        let target = root.join("mygen");
        let staging = root.join("next");
        write_installed_version(&staging, "mygen", "3.0.0", "");
        install_commit::commit_install(&staging, &target, true)
            .expect_err("ambiguous cleanup before removal");
        reap_install_backup_siblings(&root, std::time::SystemTime::now())
            .expect_err("retain original backup");
        uninstall("mygen", &root, true).expect_err("retain original and current packages");
        assert_verified_package(&record.backup, "mygen", "1.0.0");
        assert_verified_package(&target, "mygen", "2.0.0");
        assert_eq!(std::fs::read(&record.sidecar).unwrap(), metadata);
    }

    #[test]
    fn backup_cleanup_errors_converge_without_ambiguous_generations() {
        for failure in [
            "cleanup-backup",
            "cleaning-write",
            "cleanup-backup-sync",
            "cleanup-sidecar",
            "cleanup-sidecar-sync",
        ] {
            let root = test_root(failure);
            let _guard = TempGuard(root.clone());
            let target = root.join("mygen");
            let staging = root.join("staging");
            write_installed(&target, "mygen", "");
            write_installed_version(&staging, "mygen", "2.0.0", "");
            install_commit::with_backup_probe(
                move |point| {
                    anyhow::ensure!(point != failure, "injected {failure}");
                    Ok(())
                },
                || install_commit::commit_install(&staging, &target, true),
            )
            .expect_err("cleanup error");
            assert_verified_package(&target, "mygen", "2.0.0");
            let records = install_commit::backup_records(&root).expect("records");
            if failure == "cleanup-sidecar-sync" {
                assert!(records.is_empty());
            } else {
                assert_eq!(records.len(), 1);
                assert_eq!(
                    records[0].backup.exists(),
                    matches!(failure, "cleanup-backup" | "cleaning-write")
                );
                let data: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&records[0].sidecar).unwrap()).unwrap();
                assert_eq!(
                    data["phase"],
                    if matches!(failure, "cleanup-backup" | "cleaning-write") {
                        "owned"
                    } else {
                        "cleaning"
                    }
                );
                // Another attempt must stop at old cleanup, before allocating a
                // new generation, if cleanup still fails.
                write_installed_version(&staging, "mygen", "3.0.0", "");
                install_commit::with_backup_probe(
                    |point| {
                        anyhow::ensure!(point != "cleanup-backup", "still unable to clean");
                        Ok(())
                    },
                    || install_commit::commit_install(&staging, &target, true),
                )
                .expect_err("cleanup pending");
                assert_eq!(
                    install_commit::backup_records(&root)
                        .expect("records")
                        .len(),
                    1
                );
            }
            write_installed_version(&staging, "mygen", "3.0.0", "");
            install_commit::commit_install(&staging, &target, true)
                .expect("retry cleanup before replacement");
            assert_verified_package(&target, "mygen", "3.0.0");
            assert!(
                install_commit::backup_records(&root)
                    .expect("records")
                    .is_empty()
            );
        }
    }

    #[test]
    fn backup_recovery_refuses_multiple_candidates_and_invalid_identity_or_inventory() {
        let root = test_root("ambiguous-backups");
        let _guard = TempGuard(root.clone());
        for nonce in ["first", "second"] {
            let backup = root.join(format!(".install-backup-{nonce}"));
            write_installed(&backup, "mygen", "");
            install_commit::write_owned_backup_marker(&backup, "mygen").expect("record");
        }
        let error = recover_test_target(&root, "mygen").expect_err("never choose newest");
        assert!(error.to_string().contains("multiple recovery backups"));
        assert!(!root.join("mygen").exists());
        assert_eq!(
            install_commit::backup_records(&root)
                .expect("records")
                .len(),
            2
        );
        for record in install_commit::backup_records(&root).expect("records") {
            assert_verified_package(&record.backup, "mygen", "1.0.0");
            assert!(record.sidecar.exists());
        }
        // Remove ambiguity explicitly, then test verified manifest ID equality.
        let second = root.join(".install-backup-second");
        std::fs::remove_dir_all(&second).expect("operator resolves");
        std::fs::remove_file(install_commit::backup_sidecar_path(&second))
            .expect("operator resolves record");
        let first = root.join(".install-backup-first");
        write_installed(&first, "other", "");
        let error = recover_test_target(&root, "mygen").expect_err("identity mismatch");
        assert!(format!("{error:#}").contains("manifest ID differs"));
        assert!(first.exists());
        write_installed(&first, "mygen", "");
        std::fs::write(first.join("extra.txt"), "unlisted").expect("tamper inventory");
        recover_test_target(&root, "mygen").expect_err("inventory mismatch");
        assert!(!root.join("mygen").exists());
        assert!(first.exists());
    }

    #[test]
    fn backup_metadata_is_bounded_typed_exact_and_legacy_is_unknown() {
        let root = test_root("invalid-sidecars");
        let _guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let valid =
            |name: &str| serde_json::json!({"schema":2,"id":"mygen","backup":name,"phase":"owned"});
        let cases = [
            "malformed",
            "oversized",
            "unsafe",
            "slash",
            "dot",
            "mismatch",
            "schema",
            "unknown",
            "missing-phase",
            "invalid-phase",
            "intent-only-legacy",
        ];
        for case in cases {
            let basename = format!(".install-backup-{case}");
            let backup = root.join(&basename);
            write_installed(&backup, "mygen", "");
            let mut data = valid(&basename);
            match case {
                "unsafe" => data["id"] = "../escape".into(),
                "slash" => data["id"] = "nested/id".into(),
                "dot" => data["id"] = ".".into(),
                "mismatch" => data["backup"] = ".install-backup-different".into(),
                "schema" => data["schema"] = 1.into(),
                "unknown" => data["target"] = "/ignored-but-forbidden".into(),
                "missing-phase" => {
                    data.as_object_mut().unwrap().remove("phase");
                }
                "invalid-phase" => data["phase"] = "renamed".into(),
                "intent-only-legacy" => {
                    data["schema"] = 1.into();
                    data.as_object_mut().unwrap().remove("phase");
                }
                _ => {}
            }
            let bytes = match case {
                "malformed" => "{".to_string(),
                // Valid JSON with trailing whitespace: only bounding rejects it.
                "oversized" => format!("{}{}", data, " ".repeat(4096)),
                _ => data.to_string(),
            };
            std::fs::write(install_commit::backup_sidecar_path(&backup), bytes)
                .expect("invalid sidecar");
        }
        let legacy = root.join(".install-backup-legacy");
        write_installed(&legacy, "mygen", "");
        std::fs::write(legacy.join(".backup.json"), r#"{"id":"mygen"}"#).expect("legacy marker");
        let unmarked = root.join(".install-backup-unmarked");
        write_installed(&unmarked, "mygen", "");
        assert!(
            install_commit::backup_records(&root)
                .expect("no recognized records")
                .is_empty()
        );
        recover_test_target(&root, "mygen").expect("unknown retained, never restored");
        assert!(!root.join("mygen").exists());
        write_installed(&root.join("mygen"), "mygen", "");
        reap_install_backup_siblings(&root, std::time::SystemTime::now()).expect("unknown sweep");
        uninstall("mygen", &root, true).expect("uninstall retains unknown");
        for case in cases {
            let backup = root.join(format!(".install-backup-{case}"));
            assert!(backup.exists());
            assert!(install_commit::backup_sidecar_path(&backup).exists());
        }
        assert!(legacy.join(".backup.json").exists());
        assert!(unmarked.exists());
        recover_test_target(&root, "mygen").expect("no resurrection");
        assert!(!root.join("mygen").exists());
    }

    #[cfg(unix)]
    #[test]
    fn backup_symlinked_sidecars_and_backups_are_never_adopted_or_deleted() {
        use std::os::unix::fs::symlink;
        let root = test_root("symlink-sidecars");
        let _guard = TempGuard(root.clone());
        write_installed(&root.join("mygen"), "mygen", "");
        let backup = root.join(".install-backup-symlink-marker");
        write_installed(&backup, "mygen", "");
        let record = install_commit::write_owned_backup_marker(&backup, "mygen").expect("record");
        let external = root.join("external.json");
        std::fs::rename(&record.sidecar, &external).expect("move metadata");
        symlink(&external, &record.sidecar).expect("sidecar link");
        assert!(
            install_commit::write_backup_marker(&backup, "mygen").is_err(),
            "exclusive creation must not overwrite/follow link"
        );
        let linked_backup = root.join(".install-backup-symlink-tree");
        let linked_record =
            install_commit::write_owned_backup_marker(&linked_backup, "mygen").expect("record");
        symlink(&backup, &linked_backup).expect("backup link");
        assert!(
            install_commit::backup_records(&root)
                .expect("records")
                .is_empty()
        );
        reap_install_backup_siblings(&root, std::time::SystemTime::now()).expect("sweep");
        uninstall("mygen", &root, true).expect("uninstall");
        recover_test_target(&root, "mygen").expect("never adopt linked backup");
        assert!(!root.join("mygen").exists());
        assert!(
            std::fs::symlink_metadata(&record.sidecar)
                .expect("link retained")
                .is_symlink()
        );
        assert!(
            std::fs::symlink_metadata(&linked_backup)
                .expect("link retained")
                .is_symlink()
        );
        assert!(linked_record.sidecar.exists());
        assert_verified_package(&backup, "mygen", "1.0.0");
        assert!(external.exists());
    }

    #[test]
    fn backup_uninstall_cleanup_failure_keeps_target_until_safe_cleanup() {
        for failure in [
            "cleanup-backup",
            "cleaning-write",
            "cleanup-backup-sync",
            "cleanup-sidecar",
            "cleanup-sidecar-sync",
        ] {
            let root = test_root(&format!("uninstall-{failure}"));
            let _guard = TempGuard(root.clone());
            write_installed(&root.join("mygen"), "mygen", "");
            let backup = root.join(".install-backup-owned");
            write_installed(&backup, "mygen", "");
            install_commit::write_owned_backup_marker(&backup, "mygen").expect("record");
            install_commit::with_backup_probe(
                move |point| {
                    anyhow::ensure!(point != failure, "injected {failure}");
                    Ok(())
                },
                || uninstall("mygen", &root, true),
            )
            .expect_err("safe cleanup must precede target removal");
            assert_verified_package(&root.join("mygen"), "mygen", "1.0.0");
            uninstall("mygen", &root, true).expect("retry cleanup");
            recover_test_target(&root, "mygen").expect("no resurrection");
            assert!(!root.join("mygen").exists());
            assert!(!backup.exists());
            assert!(!install_commit::backup_sidecar_path(&backup).exists());
        }
    }

    #[test]
    fn backup_restore_never_replaces_an_unexpected_target() {
        let root = test_root("restore-no-replace");
        let _guard = TempGuard(root.clone());
        let backup = root.join(".install-backup-owned");
        write_installed(&backup, "mygen", "");
        install_commit::write_owned_backup_marker(&backup, "mygen").expect("record");
        let target = root.join("mygen");
        let planted_target = target.clone();
        install_commit::with_backup_probe(
            move |point| {
                if point == "restore-rename" {
                    write_installed_version(&planted_target, "mygen", "9.0.0", "");
                }
                Ok(())
            },
            || recover_test_target(&root, "mygen"),
        )
        .expect_err("no-replace must refuse");
        assert_verified_package(&target, "mygen", "9.0.0");
        assert_verified_package(&backup, "mygen", "1.0.0");
        let bytes = std::fs::read(install_commit::backup_sidecar_path(&backup)).unwrap();
        let data: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            data["phase"], "owned",
            "known no-move failure reverts ownership"
        );
    }

    #[test]
    fn backup_maintenance_skips_each_contended_lock_in_global_first_order() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("maintenance-lock-order");
        let _guard = TempGuard(root.clone());
        write_installed(&root.join("mygen"), "mygen", "");
        let must_skip = || {
            install_commit::with_backup_maintenance_lock(&root, "mygen", || {
                anyhow::bail!("maintenance entered while a preceding lock was held")
            })
            .expect("contention skips instead of waiting")
        };
        {
            let _global = install_commit::global_commit_rwlock()
                .write()
                .expect("global process");
            must_skip();
        }
        {
            let _file =
                install_commit::acquire_file_lock(&install_commit::global_lock_path(&root), &root)
                    .expect("global file");
            must_skip();
        }
        {
            let _shard = install_commit::replace_mutex_for("mygen")
                .lock()
                .expect("shard");
            must_skip();
        }
        {
            let _id = install_commit::acquire_file_lock(
                &root.join(install_commit::per_id_lock_name("mygen")),
                &root,
            )
            .expect("per-ID");
            must_skip();
        }
        let entered = std::cell::Cell::new(false);
        install_commit::with_backup_maintenance_lock(&root, "mygen", || {
            entered.set(true);
            Ok(())
        })
        .expect("uncontended");
        assert!(entered.get());
    }

    #[test]
    fn backup_sweep_verifies_target_and_retains_sidecar_on_cleanup_failure() {
        if env_process::isolated() {
            return;
        }
        let root = test_root("sweep-cleanup");
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        write_installed(&target, "mygen", "");
        let backup = root.join(".install-backup-owned");
        write_installed(&backup, "mygen", "");
        let record = install_commit::write_owned_backup_marker(&backup, "mygen").expect("record");
        std::fs::write(target.join("extra.txt"), "tampered").expect("tamper");
        reap_install_backup_siblings(&root, std::time::SystemTime::now())
            .expect_err("unverified target cannot authorize deletion");
        assert!(backup.exists());
        assert!(record.sidecar.exists());
        std::fs::remove_file(target.join("extra.txt")).expect("repair");
        install_commit::with_backup_probe(
            |point| {
                anyhow::ensure!(
                    point != "cleanup-backup-sync",
                    "injected cleanup flush error"
                );
                Ok(())
            },
            || reap_install_backup_siblings(&root, std::time::SystemTime::now()),
        )
        .expect_err("sweep reports cleanup error");
        assert!(!backup.exists());
        assert!(
            record.sidecar.exists(),
            "metadata retained until deletion flushed"
        );
        reap_install_backup_siblings(&root, std::time::SystemTime::now())
            .expect("sweep orphan sidecar");
        assert!(!record.sidecar.exists());
        assert_verified_package(&target, "mygen", "1.0.0");
    }

    #[test]
    fn backup_publish_conflict_never_overwrites_target_during_rollback() {
        let root = test_root("publish-conflict");
        let _guard = TempGuard(root.clone());
        let target = root.join("mygen");
        let staging = root.join("staging");
        write_installed(&target, "mygen", "");
        write_installed_version(&staging, "mygen", "2.0.0", "");
        let unexpected = target.clone();
        install_commit::with_backup_probe(
            move |point| {
                if point == "publish-rename" {
                    write_installed_version(&unexpected, "mygen", "9.0.0", "");
                }
                Ok(())
            },
            || install_commit::commit_install(&staging, &target, true),
        )
        .expect_err("publication and restore refuse overwrite");
        assert_verified_package(&target, "mygen", "9.0.0");
        let records = install_commit::backup_records(&root).expect("records");
        assert_eq!(records.len(), 1);
        assert_verified_package(&records[0].backup, "mygen", "1.0.0");
        assert!(records[0].sidecar.exists());
        assert_verified_package(&staging, "mygen", "2.0.0");
    }

    async fn set_registry_home(home: &Utf8Path) -> HomeGuard {
        let lock = ENV_LOCK.lock().await;
        // SAFETY: the guard serializes environment mutation across tests.
        unsafe {
            std::env::set_var("DATA_HOME", home);
        }
        HomeGuard { _lock: lock }
    }
}

#[cfg(test)]
mod env_process {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/test-support/environment.rs"
    ));
}
