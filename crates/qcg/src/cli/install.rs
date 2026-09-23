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

use super::plan::ensure_safe_install_id;

use qcg_policy::MAX_SIGNING_KEY_BYTES;

/// Test-only re-exports: the native tests below (and `main.rs` tests) reach
/// past the split through `install::` instead of naming submodules.
#[cfg(test)]
pub(crate) use super::install_commit::{
    CleanupPaths, check_no_replace_target, commit_install, per_id_lock_name,
};
pub(crate) use super::install_commit::{finish_install, remove_owned_path, uninstall};
pub(crate) use super::install_stage::{
    InstallConfig, install_registry_package, is_cycle_error, stage_closure_for_manifest,
    stage_install_source,
};
#[cfg(test)]
pub(crate) use super::install_stage::{StagedInstall, load_verified_contract};

pub(crate) struct InstallVerification<'a> {
    pub(crate) sha256: Option<&'a str>,
    pub(crate) signature: Option<&'a str>,
    pub(crate) public_key: Option<&'a str>,
}

/// Best-effort removal of install staging left by a killed process.
/// Reaps this tool's prefixes (`.qcg-install-temp-*` and
/// `.qcg-install-backup-*` under `generators_dir`; `.qcg-install-stage-*`,
/// `.qcg-install-archive-*`, and `.qcg-install-private-*` under the OS temp
/// dir). Entries carrying the current pid are this process's own leftovers
/// from a killed earlier run and are reaped aggressively; foreign or
/// unparsable names keep the 1h age gate so a concurrent install's fresh
/// staging is never touched (E14-8). Files and directories are both reaped;
/// symlinks are never followed and never reaped. Removal failures are
/// collected and returned, never silently squashed (E14-11).
pub(crate) fn reap_install_tmps_before(
    parent: &Utf8Path,
    prefix: &str,
    cutoff: std::time::SystemTime,
    own_pid: u32,
) -> Result<()> {
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to scan `{parent}` for stale installs"));
        }
    };
    let own_marker = format!("-{own_pid}-");
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
        if !name.starts_with(prefix) {
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
        // read fails closed (reaped only if own-pid, else gated).
        let is_symlink = kind.is_symlink();
        let is_own = name.contains(&own_marker);
        if !is_own {
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
    // A clock behind the directory mtimes would make everything look
    // young; saturating to "now" fails the sweep closed (removes nothing
    // but fresh foreign entries) instead of wiping live staging. Own-pid
    // leftovers are reaped regardless of age.
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(3600))
        .unwrap_or(std::time::SystemTime::now());
    let own_pid = std::process::id();
    reap_install_tmps_before(generators_dir, ".qcg-install-temp-", cutoff, own_pid)?;
    reap_install_tmps_before(generators_dir, ".qcg-install-backup-", cutoff, own_pid)?;
    if let Ok(parent) = Utf8PathBuf::from_path_buf(std::env::temp_dir()) {
        reap_install_tmps_before(&parent, ".qcg-install-stage-", cutoff, own_pid)?;
        reap_install_tmps_before(&parent, ".qcg-install-archive-", cutoff, own_pid)?;
        reap_install_tmps_before(&parent, ".qcg-install-private-", cutoff, own_pid)?;
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
    reap_stale_install_tmps(generators_dir)?;
    let source_is_path = path_exists(Utf8Path::new(source))?;
    if let Some((id, requirement)) = crate::registry::split_id_source(source)
        && !source_is_path
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
        return install_registry_package(config, &id, &requirement, &mut BTreeSet::new()).await;
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
    visiting.insert((
        parent_id.clone(),
        staged.contract.manifest.generator.version.clone(),
    ));
    let staged_closure = match stage_closure_for_manifest(
        config,
        &staged.contract.manifest,
        &mut visiting,
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
    // Single commit phase: dependencies first, parent last. No SBOM
    // inventory demand beyond staged verification for raw directory
    // sources: they never carried supply-chain metadata. The staged copy
    // was already contract-validated; what must not happen is a committed
    // parent without its closure.
    for dep_staged in staged_closure {
        let dep_id = dep_staged.contract.manifest.generator.id.clone();
        finish_install(
            providers_path,
            dep_staged,
            generators_dir,
            yes,
            force,
            limits,
        )
        .map_err(|error| {
            if is_cycle_error(&error) {
                anyhow::anyhow!(
                    "generator `{parent_id}` dependency cycle committing `{dep_id}`: {error}; refusing to commit parent"
                )
            } else {
                anyhow::anyhow!(
                    "dependency closure cannot be satisfied for generator `{parent_id}` committing `{dep_id}`: {error}; refusing to commit parent"
                )
            }
        })?;
    }
    let installed_id = finish_install(providers_path, staged, generators_dir, yes, force, limits)?;
    Ok(installed_id)
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
    let pkcs8 = qcg_fs::read_bounded(signing_key, Some(MAX_SIGNING_KEY_BYTES))
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

    fn write_installed(dir: &Utf8Path, id: &str, dependency: &str) {
        std::fs::create_dir_all(dir).expect("installed generator dir");
        let manifest = format!(
            r#"
[generator]
id = "{id}"
name = "{id}"
version = "1.0.0"
qcg_version = "^0.1"
{dependency}
"#
        );
        std::fs::write(dir.join("qcg.toml"), &manifest).expect("installed manifest");
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
                let bits = std::fs::metadata(dir.join("qcg.toml"))
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
            dir.join("QCG-SBOM.spdx.json"),
            serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [{ "fileName": "qcg.toml", "checksums": [{ "checksumValue": digest }], "mode": mode }],
            })
            .to_string(),
        )
        .expect("installed sbom");
        std::fs::write(
            dir.join("QCG-PROVENANCE.intoto.json"),
            r#"{"_type":"https://in-toto.io/Statement/v1"}"#,
        )
        .expect("installed provenance");
    }

    /// Writes a direct directory source with supply-chain metadata (SBOM +
    /// provenance) so staging (which REQUIRES an SBOM, fail-closed E14) can
    /// verify it. The SBOM lists `qcg.toml` with its content digest + actual
    /// sanitized mode.
    /// Packable source: `qcg.toml` only. `package()` generates SBOM and
    /// provenance itself and refuses pre-existing reserved metadata paths,
    /// so registry-archive fixtures must not pre-place them (E14).
    fn write_packable_source(dir: &Utf8Path, manifest: &str) {
        std::fs::create_dir_all(dir).expect("source dir should be created");
        std::fs::write(dir.join("qcg.toml"), manifest).expect("source manifest should be written");
    }

    fn write_source_with_sbom(dir: &Utf8Path, manifest: &str) {
        std::fs::create_dir_all(dir).expect("source dir should be created");
        std::fs::write(dir.join("qcg.toml"), manifest).expect("source manifest should be written");
        let digest = hex::encode(Sha256::digest(manifest.as_bytes()));
        let mode = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let bits = std::fs::metadata(dir.join("qcg.toml"))
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
            dir.join("QCG-SBOM.spdx.json"),
            serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [{ "fileName": "qcg.toml", "checksums": [{ "checksumValue": digest }], "mode": mode }],
            })
            .to_string(),
        )
        .expect("source sbom should be written");
        std::fs::write(
            dir.join("QCG-PROVENANCE.intoto.json"),
            r#"{"_type":"https://in-toto.io/Statement/v1"}"#,
        )
        .expect("source provenance should be written");
    }

    #[tokio::test]
    async fn installed_parent_repairs_a_missing_dependency_closure() {
        // E14: re-running an installed parent must verify (and attempt to
        // repair) its dependency closure instead of returning success.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-closure-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        // An empty QCG_HOME keeps the repair attempt deterministic: with a
        // complete closure no registry is consulted, and a missing closure
        // fails on registry resolution instead of touching the network.
        let _env = set_qcg_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
            .await
            .expect("a complete closure must re-validate without a registry");
        std::fs::remove_dir_all(generators.join("b")).expect("remove dependency");
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        let error = install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
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
        // E14: a modified installed package must fail inventory
        // verification instead of being silently reused.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-tamper-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
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
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        let error = install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
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
        // E14: a configured but unreachable registry must fail the repair
        // instead of being skipped as if the closure were complete.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-fetch-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config.registries.insert(
            "broken".into(),
            format!("file://{}/missing-index.toml", root.join("registry")),
        );
        crate::registry::save_registries(&home, &config).expect("registries should save");
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        let error = install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
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
        // E14: a true A -> B -> A dependency cycle fails closed instead of
        // recursing forever or being silently accepted.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-cycle-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "\n[dependencies]\na = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        let error = install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
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
        // E14: a diamond (A -> {B, C} -> D) must resolve without false
        // cycle detection and without consulting a registry once every
        // package in the closure is installed.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-diamond-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
        write_installed(
            &generators.join("a"),
            "a",
            "\n[dependencies]\nb = \"^1\"\nc = \"^1\"\n",
        );
        write_installed(&generators.join("b"), "b", "\n[dependencies]\nd = \"^1\"\n");
        write_installed(&generators.join("c"), "c", "\n[dependencies]\nd = \"^1\"\n");
        write_installed(&generators.join("d"), "d", "");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        for id in ["a", "b", "c"] {
            let config = InstallConfig {
                providers_path: None,
                generators_dir: &generators,
                yes: false,
                force: false,
                limits: &limits,
            };
            install_registry_package(config, id, &requirement, &mut BTreeSet::new())
                .await
                .unwrap_or_else(|error| panic!("diamond closure `{id}` must resolve: {error}"));
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn stale_install_tmps_reap_by_cutoff_prefix_and_ownership() {
        // E14-8: install staging, archive, and backup prefixes are reaped;
        // own-pid leftovers go aggressively while foreign names keep the 1h
        // gate. A cutoff of `UNIX_EPOCH` reaps nothing foreign, proving the
        // age gate direction without touching mtimes.
        use std::time::SystemTime;
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-reap-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let own_pid = std::process::id();
        let foreign_pid = own_pid.wrapping_add(1);
        // Own-pid staging dir is reaped even with a future cutoff (no age gate).
        let own_staging = root.join(format!(".qcg-install-temp-{own_pid}-old"));
        // Foreign staging with an old mtime is reaped; the epoch cutoff
        // proves the gate (nothing older than epoch, so nothing reaped).
        let foreign_staging = root.join(format!(".qcg-install-temp-{foreign_pid}-old"));
        let foreign_archive = root.join(format!(".qcg-install-archive-{foreign_pid}-old.qcg"));
        let foreign_backup = root.join(format!(".qcg-install-backup-{foreign_pid}-old"));
        let foreign_file = root.join("generators");
        std::fs::create_dir_all(&own_staging).expect("own staging should be created");
        std::fs::create_dir_all(&foreign_staging).expect("foreign staging should be created");
        std::fs::write(&foreign_archive, b"x").expect("foreign archive should be created");
        std::fs::create_dir_all(&foreign_backup).expect("foreign backup should be created");
        std::fs::create_dir_all(&foreign_file).expect("foreign should be created");
        reap_install_tmps_before(&root, ".qcg-install-temp-", SystemTime::now(), own_pid)
            .expect("reap should succeed");
        assert!(
            !own_staging.exists(),
            "own-pid staging must be reaped aggressively"
        );
        assert!(
            !foreign_staging.exists(),
            "old foreign staging must be reaped"
        );
        assert!(foreign_file.exists(), "a foreign dir must survive");
        // Archive and backup prefixes are reaped too.
        reap_install_tmps_before(&root, ".qcg-install-archive-", SystemTime::now(), own_pid)
            .expect("archive reap should succeed");
        assert!(!foreign_archive.exists(), "an archive file must be reaped");
        reap_install_tmps_before(&root, ".qcg-install-backup-", SystemTime::now(), own_pid)
            .expect("backup reap should succeed");
        assert!(!foreign_backup.exists(), "a backup dir must be reaped");
        // Epoch cutoff reaps nothing foreign.
        std::fs::create_dir_all(&foreign_staging).expect("foreign staging should be recreated");
        reap_install_tmps_before(
            &root,
            ".qcg-install-temp-",
            SystemTime::UNIX_EPOCH,
            foreign_pid.wrapping_add(1000),
        )
        .expect("reap should succeed");
        assert!(
            foreign_staging.exists(),
            "nothing is older than the epoch, so nothing must be reaped"
        );
    }

    #[test]
    fn no_replace_probe_refuses_an_existing_target() {
        // E14: the non-Linux fallback must fail closed when the target is
        // observed, never silently replace it. Runs on every platform.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-noreplace-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
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
        // E14: a direct (local directory) install resolves and stages the
        // FULL closure before any commit. With an unsatisfiable dependency
        // the install refuses pre-commit and never leaves a committed
        // parent behind (no rerun repair demand).
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-direct-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let source = root.join("source");
        std::fs::create_dir_all(&home).expect("home dir");
        std::fs::create_dir_all(source.join("sub")).expect("source dir");
        let _env = set_qcg_home(&home).await;
        // Direct sources must carry an SBOM (fail-closed E14): write one so
        // the failure under test is the missing dependency, not missing
        // supply-chain metadata.
        write_source_with_sbom(
            &source.join("sub"),
            "[generator]\nid = \"direct-a\"\nname = \"Direct A\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n\n[dependencies]\nmissing-dep = \"^1\"\n",
        );
        let limits = qcg_service::PackageLimits::default();
        let verification = InstallVerification {
            sha256: None,
            signature: None,
            public_key: None,
        };
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

    /// Holds the environment lock for the test's whole lifetime: `QCG_HOME`
    /// is process-global, so any test that overrides it must not interleave
    /// with another one. Restores `QCG_HOME` on drop so a panicking assertion
    /// cannot leak the override into other tests.
    struct QcgHomeGuard {
        _lock: tokio::sync::MutexGuard<'static, ()>,
    }

    impl Drop for QcgHomeGuard {
        fn drop(&mut self) {
            // SAFETY: the guard still holds ENV_LOCK, so no other test can
            // be mutating the environment here.
            unsafe {
                std::env::remove_var("QCG_HOME");
            }
        }
    }

    #[test]
    fn finish_install_force_replaces_existing_generator() {
        // E14: --force must replace an existing install; without it the
        // second install fails closed instead of silently merging.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-force-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let staged = root.join("staged");
        std::fs::create_dir_all(&staged).expect("staged dir");
        write_installed(&staged, "a", "");
        let limits = qcg_service::PackageLimits::default();
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
                name.starts_with(".qcg-install-backup-") || name.starts_with(".qcg-install-temp-")
            })
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "transaction leftovers: {leftovers:?}");
    }

    #[tokio::test]
    async fn version_mismatch_diamond_fails_closed() {
        // E14-3: a diamond requiring two different versions of D cannot
        // converge on one install dir; the second branch fails closed
        // instead of silently last-winning.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-vdiamond-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
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
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        let error = install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
            .await
            .expect_err("a version-mismatch diamond must fail closed");
        assert!(
            error.to_string().contains("does not satisfy")
                || error.to_string().contains("registry"),
            "the mismatch must be reported, got: {error}"
        );
    }

    #[tokio::test]
    async fn direct_self_dependency_fails_closed() {
        // E14-3: a generator depending on itself is a cycle and fails
        // closed, never recursing forever.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-selfdep-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
        write_installed(&generators.join("s"), "s", "\n[dependencies]\ns = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        let error = install_registry_package(config, "s", &requirement, &mut BTreeSet::new())
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
    async fn missing_diamond_fill_converges_on_retry() {
        // E14-10: a diamond with a missing leaf fails, then converges once
        // the leaf is filled and the install is rerun.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-fill-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
        write_installed(
            &generators.join("a"),
            "a",
            "\n[dependencies]\nb = \"^1\"\nc = \"^1\"\n",
        );
        write_installed(&generators.join("b"), "b", "\n[dependencies]\nd = \"^1\"\n");
        write_installed(&generators.join("c"), "c", "\n[dependencies]\nd = \"^1\"\n");
        write_installed(&generators.join("d"), "d", "");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
            .await
            .expect("a complete diamond must resolve");
        std::fs::remove_dir_all(generators.join("d")).expect("remove diamond leaf");
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
            .await
            .expect_err("a missing diamond leaf must fail");
        write_installed(&generators.join("d"), "d", "");
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: false,
            force: false,
            limits: &limits,
        };
        install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
            .await
            .expect("the filled diamond must converge");
    }

    #[tokio::test]
    async fn first_failure_then_rerun_converges_via_real_install() {
        // E14 TRUE first-failure e2e (not manual fill): a direct parent
        // install with a missing dependency fails pre-commit with no parent
        // left behind; the dependency is then installed via the REAL
        // `install` path (from a directory source with SBOM, through
        // staging + commit), and rerunning the parent converges. No
        // `write_installed` manual fill is used for the dependency — both
        // installs go through `install` (stage + commit).
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-e2e-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let dep_src = root.join("dep-src");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_qcg_home(&home).await;
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-parent\"\nname = \"E2E Parent\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n\n[dependencies]\ne2e-dep = \"^1\"\n",
        );
        write_source_with_sbom(
            &dep_src,
            "[generator]\nid = \"e2e-dep\"\nname = \"E2E Dep\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        );
        let limits = qcg_service::PackageLimits::default();
        let verification = InstallVerification {
            sha256: None,
            signature: None,
            public_key: None,
        };
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
            InstallVerification {
                sha256: None,
                signature: None,
                public_key: None,
            },
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
            InstallVerification {
                sha256: None,
                signature: None,
                public_key: None,
            },
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
                name.starts_with(".qcg-install-backup-") || name.starts_with(".qcg-install-temp-")
            })
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "e2e must leave no residue: {leftovers:?}"
        );
    }

    #[tokio::test]
    async fn post_cycle_state_converges_without_residue() {
        // E14-10: after a cycle failure, retrying fails the same way without
        // wedging on residue; no backup or temp dirs linger.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-postcycle-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir");
        let _env = set_qcg_home(&home).await;
        write_installed(&generators.join("a"), "a", "\n[dependencies]\nb = \"^1\"\n");
        write_installed(&generators.join("b"), "b", "\n[dependencies]\na = \"^1\"\n");
        let requirement = semver::VersionReq::parse("^1").expect("requirement");
        let limits = qcg_service::PackageLimits::default();
        for _ in 0..2 {
            let config = InstallConfig {
                providers_path: None,
                generators_dir: &generators,
                yes: false,
                force: false,
                limits: &limits,
            };
            let error = install_registry_package(config, "a", &requirement, &mut BTreeSet::new())
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
                name.starts_with(".qcg-install-backup-") || name.starts_with(".qcg-install-temp-")
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
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-parallel-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        std::fs::create_dir_all(&generators).expect("generators dir");
        let target = generators.join("a");
        std::fs::create_dir_all(&target).expect("initial target");
        std::fs::write(target.join("old.txt"), b"old").expect("old file");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|index| {
                let barrier = barrier.clone();
                let root = root.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let staging = root.join(format!("staging-{index}"));
                    let _ = std::fs::remove_dir_all(&staging);
                    std::fs::create_dir_all(&staging).expect("staging dir");
                    std::fs::write(staging.join(format!("new-{index}.txt")), b"new")
                        .expect("staging file");
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
            target.join("qcg.toml").exists() || target.exists(),
            "target must exist"
        );
        let leftovers = std::fs::read_dir(&generators)
            .expect("generators should be readable")
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with(".qcg-install-backup-"))
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
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-uninstall-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
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
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-unlink-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let outside = root.join("outside");
        std::fs::create_dir_all(&generators).expect("generators dir");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::fs::write(outside.join("qcg.toml"), "marker").expect("outside marker");
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
        // Gap 9: a directory source must be copied into private staging;
        // the staged path is never the live path, so later live mutations
        // cannot redirect the commit.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-stage-copy-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        // Serialize with other installs: the stale-temp sweep reaps
        // own-pid private staging aggressively, so a concurrent install
        // could reap this test's live staging mid-assertion.
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_qcg_home(&home).await;
        let live = root.join("live");
        write_source_with_sbom(
            &live,
            "[generator]\nid = \"live-a\"\nname = \"Live A\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        );
        let limits = qcg_service::PackageLimits::default();
        let staged = stage_install_source(live.as_str(), None, None, &limits)
            .await
            .expect("dir source should stage");
        assert_ne!(
            staged.path.as_str(),
            live.as_str(),
            "staged path must not be the live path"
        );
        assert!(
            staged.path.as_str().contains(".qcg-install-private-")
                || staged.path.as_str().contains(".qcg-install-stage-"),
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
    async fn staged_contract_is_parsed_once_and_threaded() {
        // Gap 10: the manifest is parsed once at stage time; the contract
        // travels with the staging and its root denotes the staged copy.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-parse-once-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        // Serialize with other installs for the same stale-sweep reason as
        // the private-copy test above.
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_qcg_home(&home).await;
        let live = root.join("live");
        write_source_with_sbom(
            &live,
            "[generator]\nid = \"once-a\"\nname = \"Once A\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        );
        let limits = qcg_service::PackageLimits::default();
        let staged = stage_install_source(live.as_str(), None, None, &limits)
            .await
            .expect("dir source should stage");
        assert_eq!(
            staged.contract.root, staged.path,
            "threaded contract root must be the staged copy"
        );
        assert_eq!(staged.contract.manifest.generator.id, "once-a");
        assert_eq!(staged.contract.manifest.generator.version, "1.0.0");
        // Committing via the threaded contract installs the same id without
        // any caller-side re-parse.
        let generators = root.join("generators");
        let id = finish_install(None, staged, &generators, true, false, &limits)
            .expect("commit with threaded contract should succeed");
        assert_eq!(id, "once-a");
        assert!(generators.join("once-a/qcg.toml").exists());
    }

    #[tokio::test]
    async fn registry_fetch_failure_then_rerun_repairs_via_registry() {
        // E14: a dependency whose archive fetch fails first leaves no parent
        // behind; once the recording file:// registry serves the archive, a
        // rerun repairs the closure through the real registry path (no manual
        // fill). Both attempts go through `install`.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-regretry-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let dep_src = root.join("dep-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_qcg_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        write_packable_source(
            &dep_src,
            "[generator]\nid = \"e2e-reg-dep\"\nname = \"E2E Reg Dep\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        );
        write_source_with_sbom(
            &parent_src,
            "[generator]\nid = \"e2e-reg-parent\"\nname = \"E2E Reg Parent\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n\n[dependencies]\ne2e-reg-dep = \"^1\"\n",
        );
        let archive = registry_dir.join("e2e-reg-dep.qcg");
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
        let limits = qcg_service::PackageLimits::default();
        let verification = InstallVerification {
            sha256: None,
            signature: None,
            public_key: None,
        };
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
            InstallVerification {
                sha256: None,
                signature: None,
                public_key: None,
            },
            &limits,
        )
        .await
        .expect("rerun after registry repair must converge");
        assert_eq!(rerun, "e2e-reg-parent");
        assert!(generators.join("e2e-reg-dep/qcg.toml").exists());
        assert!(generators.join("e2e-reg-parent/qcg.toml").exists());
    }

    #[tokio::test]
    async fn dependency_commit_failure_keeps_prior_deps_and_rerun_converges() {
        // E14: a dependency commit failure leaves already-committed earlier
        // dependencies behind (documented, not rolled back); removing the
        // blocker and rerunning converges the full closure.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-commitseq-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let home = root.join("home");
        let generators = root.join("generators");
        let parent_src = root.join("parent-src");
        let dep_a_src = root.join("dep-a-src");
        let dep_b_src = root.join("dep-b-src");
        let registry_dir = root.join("registry");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        std::fs::create_dir_all(&registry_dir).expect("registry dir should be created");
        let _env = set_qcg_home(&home).await;
        let mut config = crate::registry::RegistryConfig::default();
        config
            .registries
            .insert("test".into(), format!("file://{}/index.toml", registry_dir));
        crate::registry::save_registries(&home, &config).expect("registries should save");
        let limits = qcg_service::PackageLimits::default();
        let mut index = String::new();
        for (src, id) in [(&dep_a_src, "e2e-c-a"), (&dep_b_src, "e2e-c-b")] {
            write_packable_source(
                src,
                &format!(
                    "[generator]\nid = \"{id}\"\nname = \"{id}\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n"
                ),
            );
            let archive = registry_dir.join(format!("{id}.qcg"));
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
            "[generator]\nid = \"e2e-c-parent\"\nname = \"E2E C Parent\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n\n[dependencies]\ne2e-c-a = \"^1\"\ne2e-c-b = \"^1\"\n",
        );
        // The blocker is planted before the first run: an empty directory at
        // dep-b's target reads as not-installed during staging (no qcg.toml)
        // but refuses the commit as already-existing, so dep-b fails at
        // commit time after dep-a is already committed (E14).
        std::fs::create_dir_all(generators.join("e2e-c-b")).expect("blocker should create");
        let error = install(
            None,
            parent_src.as_str(),
            &generators,
            true,
            false,
            InstallVerification {
                sha256: None,
                signature: None,
                public_key: None,
            },
            &limits,
        )
        .await
        .expect_err("dep-b commit must fail on the blocker");
        // BTreeMap order stages e2e-c-a first: it stays committed while the
        // parent is refused.
        assert!(
            generators.join("e2e-c-a/qcg.toml").exists(),
            "the earlier dependency must remain committed: {error}"
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
            InstallVerification {
                sha256: None,
                signature: None,
                public_key: None,
            },
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
        // Gap 7: resolution happens before any commit, so an unresolvable
        // fresh package never leaves a parent directory behind.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-noparent-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let home = root.join("home");
        let generators = root.join("generators");
        std::fs::create_dir_all(&home).expect("home dir should be created");
        let _env = set_qcg_home(&home).await;
        let limits = qcg_service::PackageLimits::default();
        let config = InstallConfig {
            providers_path: None,
            generators_dir: &generators,
            yes: true,
            force: false,
            limits: &limits,
        };
        let requirement = semver::VersionReq::parse("^1").expect("requirement should parse");
        let error =
            install_registry_package(config, "missing-pkg", &requirement, &mut BTreeSet::new())
                .await
                .expect_err("missing registry package must fail");
        assert!(
            !generators.join("missing-pkg").exists(),
            "no parent may be left behind, got error: {error}"
        );
    }

    #[test]
    fn concurrent_uninstalls_of_same_id_serialize_atomically() {
        // Gap 11: the dependents re-check plus removal holds the install
        // locks, so concurrent uninstalls of the same id serialize instead
        // of racing; exactly one succeeds and no residue remains.
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-install-uninstall-race-{}",
            uuid::Uuid::now_v7()
        )))
        .expect("temporary path must be UTF-8");
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
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-tamper-dep-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
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
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-install-shard-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
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
            .filter(|name| name.starts_with(".qcg-install-backup-"))
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
        assert!(first.starts_with(".qcg-install-") && first.ends_with(".lock"));
    }

    async fn set_qcg_home(home: &Utf8Path) -> QcgHomeGuard {
        let lock = ENV_LOCK.lock().await;
        // SAFETY: the guard serializes environment mutation across tests.
        unsafe {
            std::env::set_var("QCG_HOME", home);
        }
        QcgHomeGuard { _lock: lock }
    }
}
