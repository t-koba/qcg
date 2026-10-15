//! Per-run generator bytes. Locations are derived, never journaled paths.
use camino::{Utf8Path, Utf8PathBuf};
use contract::Contract;

use crate::{
    package::{PackageLimits, copy_dir_all, package_identity_sha256},
    types::ServiceError,
};

pub(crate) fn package_dir(run_dir: &Utf8Path) -> Utf8PathBuf {
    run_dir.join("generator-package")
}

pub(crate) fn load(run_dir: &Utf8Path, state: &engine::RunState) -> Result<Contract, ServiceError> {
    load_identity(
        run_dir,
        state.package_sha256.as_deref(),
        state.contract_sha256.as_deref(),
    )
}

pub(crate) fn load_identity(
    run_dir: &Utf8Path,
    package_sha256: Option<&str>,
    contract_sha256: Option<&str>,
) -> Result<Contract, ServiceError> {
    let expected = package_sha256.ok_or_else(|| {
        ServiceError::Invalid(
            "run has no pinned generator package identity; refusing execution".into(),
        )
    })?;
    let root = package_dir(run_dir);
    verify(&root, expected)?;
    let contract =
        Contract::load(&root).map_err(|error| ServiceError::Invalid(error.to_string()))?;
    if contract_sha256 != Some(contract.sha256.as_str()) {
        return Err(ServiceError::Invalid(
            "pinned generator contract identity mismatch".into(),
        ));
    }
    Ok(contract)
}

pub(crate) fn verify(root: &Utf8Path, expected: &str) -> Result<(), ServiceError> {
    if expected.len() != 64 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ServiceError::Invalid(
            "invalid pinned generator package identity".into(),
        ));
    }
    let actual = package_identity_sha256(root, &PackageLimits::default())?;
    if actual != expected {
        return Err(ServiceError::Invalid(
            "pinned generator package identity mismatch".into(),
        ));
    }
    Ok(())
}

/// Caller owns the fresh run directory and cleans it on any failure.
pub(crate) fn copy(
    source: &Utf8Path,
    run_dir: &Utf8Path,
    expected: &str,
) -> Result<Contract, ServiceError> {
    let target = package_dir(run_dir);
    std::fs::create_dir(&target)?;
    copy_dir_all(source, &target, &PackageLimits::default()).map_err(|error| {
        ServiceError::Invalid(format!("generator package snapshot copy failed: {error}"))
    })?;
    verify(&target, expected).map_err(|error| {
        ServiceError::Invalid(format!(
            "generator package snapshot verification failed: {error}"
        ))
    })?;
    Contract::load(&target).map_err(|error| {
        ServiceError::Invalid(format!(
            "generator package snapshot contract load failed: {error}"
        ))
    })
}
