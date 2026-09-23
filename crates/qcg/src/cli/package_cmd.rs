use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::Contract;
use qcg_policy::GENERATED_PACKAGE_ENTRIES;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Write;
use std::time::SystemTime;
use zip::write::SimpleFileOptions;

struct PackageFileEntry {
    archive_path: String,
    source_path: Utf8PathBuf,
    bytes: u64,
    sha256: String,
    metadata: std::fs::Metadata,
}

pub(crate) fn package(
    dir: &Utf8Path,
    output: &Utf8Path,
    limits: &qcg_service::PackageLimits,
) -> Result<()> {
    const SBOM_PATH: &str = "QCG-SBOM.spdx.json";
    const PROVENANCE_PATH: &str = "QCG-PROVENANCE.intoto.json";
    let root = dunce::canonicalize(dir)
        .with_context(|| format!("failed to canonicalize package input `{dir}`"))?;
    let output_absolute = if output.is_absolute() {
        output.to_path_buf().into_std_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let output_parent = output_absolute
        .parent()
        .context("package output must have a parent directory")?;
    let output_parent = dunce::canonicalize(output_parent).with_context(|| {
        format!(
            "failed to canonicalize package output directory `{}`",
            output_parent.display()
        )
    })?;
    if output_parent.starts_with(&root) {
        anyhow::bail!("package output must be outside the package input directory");
    }
    let contract = Contract::load(dir)?;
    let mut directories = Vec::<(String, std::fs::Metadata)>::new();
    let mut entries = Vec::<PackageFileEntry>::new();
    let mut total_bytes = 0_u64;
    for entry in qcg_fs::WalkDir::new(dir) {
        let entry = entry?;
        if entry.file_type().is_symlink() {
            anyhow::bail!("package input contains a symbolic link: {}", entry.path());
        }
        // Each file opens O_NOFOLLOW at use time (open_input_nofollow
        // below, sha256_file, copy_file_with_sha256), so a terminal symlink
        // swapped in after this enumeration is refused at open. Length and
        // mode gates read from the opened handle, never from a pre-open
        // following stat. Content swapped between the hash and the archive
        // copy is caught by the byte/sha re-check. A parent directory
        // swapped between walk and open is NOT refused by these path opens
        // (single-process pack is the supported boundary; concurrent
        // workspace modification is unsupported) (E13/E15).
        let path = entry.path().to_path_buf();
        let relative = path.strip_prefix(dir)?;
        if relative.as_str().is_empty() {
            continue;
        }
        let name = qcg_policy::portable_relative_path(relative);
        if matches!(name.as_str(), SBOM_PATH | PROVENANCE_PATH) {
            anyhow::bail!("package input uses reserved metadata path `{name}`");
        }
        if limits.max_entries.is_some_and(|limit| {
            directories.len().saturating_add(entries.len())
                >= limit.saturating_sub(GENERATED_PACKAGE_ENTRIES)
        }) {
            let Some(limit) = limits.max_entries else {
                anyhow::bail!("package entry limit is missing");
            };
            anyhow::bail!("package input contains too many entries: exceeds {limit}");
        }
        // Open first (O_NOFOLLOW at use time): the length gate and the
        // recorded mode both derive from the opened handle's metadata, not
        // from a pre-open stat that a swap could invalidate (E15).
        // Directories are recorded from lstat without opening: opening a
        // directory as a file fails on Windows, and entry metadata already
        // classifies them without following symlinks.
        if entry.file_type().is_dir() {
            let metadata = std::fs::symlink_metadata(&path)?;
            directories.push((name, metadata));
            continue;
        }
        let handle = open_input_nofollow(&path)?;
        let metadata = handle.metadata()?;
        drop(handle);
        if !entry.file_type().is_file() {
            anyhow::bail!("package input contains an unsupported entry: {path}");
        }
        let bytes = metadata.len();
        total_bytes = total_bytes
            .checked_add(bytes)
            .context("package input size overflowed")?;
        if limits.max_bytes.is_some_and(|limit| total_bytes > limit) {
            let Some(limit) = limits.max_bytes else {
                anyhow::bail!("package byte limit is missing");
            };
            anyhow::bail!("package input exceeds {limit} bytes");
        }
        // The per-file hash enforces the same byte bound as the total gate
        // above: a file larger than the whole allowance fails fast instead
        // of hashing unboundedly (E15).
        let sha256 = sha256_file(&path, limits.max_bytes)?;
        entries.push(PackageFileEntry {
            archive_path: name,
            source_path: path,
            bytes,
            sha256,
            metadata,
        });
    }
    directories.sort_by(|left, right| left.0.cmp(&right.0));
    entries.sort_by(|left, right| left.archive_path.cmp(&right.archive_path));
    let files = entries
        .iter()
        .map(|entry| {
            json!({
                "fileName": entry.archive_path,
                "checksums": [{"algorithm": "SHA256", "checksumValue": entry.sha256}],
                "size": entry.bytes,
                // Sanitized permission bits, verified on unpack alongside
                // the hash: modes round-trip explicitly, never by umask or
                // silent default (E15).
                "mode": sanitized_source_mode(&entry.metadata),
            })
        })
        .collect::<Vec<_>>();
    let sbom = serde_json::to_vec_pretty(&json!({
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": format!("{}-{}", contract.manifest.generator.id, contract.manifest.generator.version),
        "documentNamespace": format!("https://qcg.local/spdx/{}/{}", contract.manifest.generator.id, contract.sha256),
        "files": files,
    }))?;
    let materials = entries
        .iter()
        .map(|entry| json!({"uri": entry.archive_path, "digest": {"sha256": entry.sha256}}))
        .collect::<Vec<_>>();
    let provenance = serde_json::to_vec_pretty(&json!({
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [{
            "name": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
            "digest": {"sha256": contract.sha256}
        }],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": "https://qcg.local/package/v1",
                "externalParameters": {},
                "internalParameters": {},
                "resolvedDependencies": materials
            },
            "runDetails": {
                "builder": {"id": format!("qcg/{}", env!("CARGO_PKG_VERSION"))},
                "metadata": {"invocationId": contract.sha256}
            }
        }
    }))?;
    // Stage + rename through a local helper that applies the final 0644
    // BEFORE the rename: the output is never truncated in place, a
    // terminal symlink at the output is refused fail-closed (never
    // followed), and no post-rename chmod ever runs, so a crash can never
    // leave a published package with the 0600 staging mode (E15).
    stage_package_output(output, |file| {
        write_package_archive(
            file,
            &directories,
            &entries,
            &sbom,
            &provenance,
            limits.max_bytes,
        )
        .map_err(|error| std::io::Error::other(error.to_string()))
    })?;
    Ok(())
}

/// Sanitized permission bits recorded per file in the SBOM and verified on
/// unpack: setuid/setgid/sticky never survive and world-writable is
/// cleared, symmetric with the unpack sanitizer (E15). Non-Unix maps the
/// read-only flag onto the explicit file modes (platform limitation).
fn sanitized_source_mode(metadata: &std::fs::Metadata) -> u32 {
    qcg_fs::sanitized_metadata_mode(metadata)
}

/// Stages the package archive in an owner-only sibling temp, applies the
/// final 0644 BEFORE the rename (pre-rename chmod), syncs, and publishes
/// by rename plus directory sync. Unix runs handle-relative to a pinned
/// parent fd (`O_NOFOLLOW` throughout); non-Unix re-validates the leaf
/// before the rename with a documented residual window (E15). Never a
/// post-rename chmod.
fn stage_package_output(
    output: &Utf8Path,
    write: impl FnOnce(&mut File) -> Result<(), std::io::Error>,
) -> Result<()> {
    #[cfg(unix)]
    {
        // Shared staging dance (E15): pin the parent here, stage/publish
        // through `qcg_fs::stage_file_at` so pack and unpack cannot drift.
        use std::os::unix::ffi::OsStrExt as _;
        let parent = output
            .parent()
            .context("package output must have a parent directory")?;
        let file_name = output
            .file_name()
            .context("package output must have a file name")?;
        let owned = {
            let canonical = dunce::canonicalize(parent).with_context(|| {
                format!("failed to canonicalize package output directory `{parent}`")
            })?;
            std::ffi::CString::new(canonical.as_os_str().as_bytes())
                .context("package output path contains a NUL byte")?
        };
        // SAFETY: the parent path is borrowed for the open call only.
        let parent_fd = unsafe {
            libc::open(
                owned.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if parent_fd < 0 {
            anyhow::bail!(
                "failed to pin package output directory `{parent}`: {}",
                std::io::Error::last_os_error()
            );
        }
        // Closed on every exit below.
        struct FdGuard(std::os::unix::io::RawFd);
        impl Drop for FdGuard {
            fn drop(&mut self) {
                // SAFETY: the fd is owned by this guard.
                unsafe {
                    libc::close(self.0);
                }
            }
        }
        let parent_guard = FdGuard(parent_fd);
        // The published package is conventionally world-readable (0644);
        // the mode lands pre-rename inside the shared helper (E15).
        qcg_fs::stage_file_at(parent_guard.0, file_name, 0o644, 100, write)?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let file_name = output
            .file_name()
            .context("package output must have a file name")?;
        let parent = output
            .parent()
            .context("package output must have a parent directory")?;
        for _ in 0..100 {
            let staging = parent.join(format!(
                ".{file_name}.qcg-part-{}",
                uuid::Uuid::now_v7().as_simple()
            ));
            if std::fs::symlink_metadata(&staging).is_ok() {
                continue;
            }
            let mut file = match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            };
            let outcome = write(&mut file).and_then(|()| file.sync_all());
            drop(file);
            if let Err(error) = outcome {
                let _ = std::fs::remove_file(&staging);
                return Err(error.into());
            }
            // Pre-rename mode on the staging path (never post-rename). The
            // staging name was just created exclusively by this call; a
            // residual symlink-plant window remains (documented) and
            // untrusted concurrent writers are unsupported on non-Unix.
            match std::fs::symlink_metadata(&staging) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    let _ = std::fs::remove_file(&staging);
                    anyhow::bail!("refusing to stage through symbolic link `{staging}`");
                }
                Ok(_) => {}
                Err(error) => {
                    let _ = std::fs::remove_file(&staging);
                    return Err(error.into());
                }
            }
            let permissions = std::fs::metadata(&staging)?.permissions();
            // The staging file was just created exclusively by this call,
            // so it is already writable; never touch the read-only flag
            // here (on Unix that would widen modes, on Windows it is a
            // no-op for fresh files).
            debug_assert!(!permissions.readonly());
            std::fs::set_permissions(&staging, permissions)?;
            match std::fs::symlink_metadata(output) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    let _ = std::fs::remove_file(&staging);
                    anyhow::bail!("refusing to replace symbolic link `{output}`");
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    let _ = std::fs::remove_file(&staging);
                    return Err(error.into());
                }
            }
            if let Err(error) = std::fs::rename(&staging, output) {
                let _ = std::fs::remove_file(&staging);
                return Err(error.into());
            }
            return Ok(());
        }
        anyhow::bail!("package staging collided repeatedly; refusing to overwrite")
    }
}

fn write_package_archive(
    file: &mut File,
    directories: &[(String, std::fs::Metadata)],
    entries: &[PackageFileEntry],
    sbom: &[u8],
    provenance: &[u8],
    max_bytes: Option<u64>,
) -> Result<()> {
    const SBOM_PATH: &str = "QCG-SBOM.spdx.json";
    const PROVENANCE_PATH: &str = "QCG-PROVENANCE.intoto.json";
    let mut zip = zip::ZipWriter::new(file);
    for (name, metadata) in directories {
        zip.add_directory(format!("{name}/"), package_zip_options(metadata, true)?)?;
    }
    for entry in entries {
        zip.start_file(
            &entry.archive_path,
            package_zip_options(&entry.metadata, false)?,
        )?;
        let (bytes, sha256) = copy_file_with_sha256(&entry.source_path, &mut zip, max_bytes)?;
        if bytes != entry.bytes || sha256 != entry.sha256 {
            anyhow::bail!(
                "package input changed while being archived: {}",
                entry.source_path
            );
        }
    }
    // Deterministic SBOM/provenance mtime so archive hashes are stable
    // across builds (E15-7). 2020-01-01 is inside the ZIP DateTime range.
    let generated_options = generated_package_zip_options(deterministic_package_time())?;
    for (name, bytes) in [(SBOM_PATH, sbom), (PROVENANCE_PATH, provenance)] {
        zip.start_file(name, generated_options)?;
        zip.write_all(bytes)?;
    }
    zip.finish()?;
    Ok(())
}

/// Fixed generation timestamp for SBOM/provenance entries so repeated packs
/// of identical inputs hash identically (E15-7).
fn deterministic_package_time() -> SystemTime {
    // 2020-01-01T00:00:00Z as Unix seconds.
    const DETERMINISTIC_EPOCH_SECS: u64 = 1_577_836_800;
    SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(DETERMINISTIC_EPOCH_SECS)
}

fn package_zip_options(metadata: &std::fs::Metadata, directory: bool) -> Result<SimpleFileOptions> {
    let mut options = SimpleFileOptions::default().compression_method(if directory {
        zip::CompressionMethod::Stored
    } else {
        zip::CompressionMethod::Deflated
    });
    options = with_zip_modified_time(options, metadata.modified()?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        // Sanitize like unpack via the single shared helper (E15).
        let mode = qcg_fs::sanitize_mode_bits(metadata.permissions().mode());
        options = options.unix_permissions(mode);
    }
    #[cfg(not(unix))]
    {
        let writable = !metadata.permissions().readonly();
        let permissions = match (directory, writable) {
            (true, true) => 0o755,
            (true, false) => 0o555,
            (false, true) => 0o644,
            (false, false) => 0o444,
        };
        options = options.unix_permissions(permissions);
    }
    Ok(options)
}

fn generated_package_zip_options(modified: SystemTime) -> Result<SimpleFileOptions> {
    with_zip_modified_time(
        SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o644),
        modified,
    )
}

fn with_zip_modified_time(
    mut options: SimpleFileOptions,
    modified: SystemTime,
) -> Result<SimpleFileOptions> {
    let modified = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
    let modified = zip::DateTime::try_from(modified).map_err(|error| {
        anyhow::anyhow!("modification time is not representable in ZIP: {error}")
    })?;
    options = options.last_modified_time(modified);
    Ok(options)
}

pub(crate) fn sha256_file(path: &Utf8Path, max_bytes: Option<u64>) -> Result<String> {
    // Open O_NOFOLLOW at use time: the WalkDir type was observed earlier and
    // the path could have been swapped to a symlink since (E15-6). The
    // remaining window (content swapped between hash and archive) is closed
    // by the byte/sha re-check after copying. Streaming keeps memory flat;
    // the byte cap fails closed if a swap grows the file past the limit.
    let file = open_input_nofollow(path)?;
    let mut file = file;
    let (hex, _) = qcg_fs::hash_opened_file_sha256(&mut file, max_bytes, |limit| {
        std::io::Error::other(format!("package input exceeds {limit} bytes while hashing"))
    })
    .with_context(|| format!("failed to hash package input `{path}`"))?;
    Ok(hex)
}

/// Opens a pack input without following a terminal symlink on Unix
/// (O_NOFOLLOW at use time, E15-6). Non-Unix pre-checks for a symlink.
fn open_input_nofollow(path: &Utf8Path) -> Result<File, std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("refusing to open symbolic link `{path}`"),
            )),
            Ok(_) => std::fs::File::open(path),
            Err(error) => Err(error),
        }
    }
}

fn copy_file_with_sha256<W: Write>(
    path: &Utf8Path,
    writer: &mut W,
    max_bytes: Option<u64>,
) -> Result<(u64, String)> {
    // O_NOFOLLOW open mirrors sha256_file above; the caller keeps the
    // byte/sha difference detection for content swapped between the two
    // reads. Residual directory-swap window is documented on the pack walk.
    let mut file = open_input_nofollow(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("package input size overflowed while archiving")?;
        if let Some(limit) = max_bytes
            && bytes > limit
        {
            anyhow::bail!("package input exceeds {limit} bytes while archiving");
        }
    }
    Ok((bytes, hex::encode(digest.finalize())))
}

#[cfg(test)]
mod tests {
    // Every test in this module is Unix-gated (executable-bit behavior),
    // so the parent import is Unix-only too.
    #[cfg(unix)]
    use super::*;

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04). A removal failure warns instead of being silently
    /// ignored. Unix executable-bit tests only.
    #[cfg(unix)]
    struct TempGuard(Utf8PathBuf);
    #[cfg(unix)]
    impl Drop for TempGuard {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(self.0.as_std_path()) {
                eprintln!("test temp cleanup failed for {}: {error}", self.0);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn pack_then_unpack_round_trips_executable_bits() {
        // E15: pack -> unpack E2E. A 0755 script stays executable, 0644
        // data stays non-executable, and setuid bits never survive.
        use std::os::unix::fs::PermissionsExt as _;
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-pack-e2e-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let input = root.join("input");
        std::fs::create_dir_all(input.join("bin")).expect("input dirs");
        std::fs::write(
            input.join("qcg.toml"),
            "[generator]\nid = \"e2e\"\nname = \"E2E\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        )
        .expect("manifest");
        std::fs::write(input.join("bin/run.sh"), "#!/bin/sh\n").expect("script");
        std::fs::write(input.join("data.txt"), "data").expect("data");
        std::fs::set_permissions(
            input.join("bin/run.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("script mode");
        std::fs::set_permissions(
            input.join("data.txt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("data mode");
        let archive = root.join("pkg.qcg");
        package(&input, &archive, &qcg_service::PackageLimits::default())
            .expect("pack should succeed");
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target dir");
        qcg_service::package::unpack_qcg(&archive, &target, &qcg_service::PackageLimits::default())
            .expect("unpack should succeed");
        let script_mode = std::fs::metadata(target.join("bin/run.sh"))
            .expect("script metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(script_mode, 0o755, "script must stay executable");
        let data_mode = std::fs::metadata(target.join("data.txt"))
            .expect("data metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(data_mode, 0o644, "data must stay non-executable");
        // setuid/setgid/sticky bits are recorded sanitized and never
        // restored, independent of the hash check (E15).
        std::fs::set_permissions(
            input.join("bin/run.sh"),
            std::fs::Permissions::from_mode(0o4755),
        )
        .expect("setuid mode");
        let archive_setuid = root.join("pkg-setuid.qcg");
        package(
            &input,
            &archive_setuid,
            &qcg_service::PackageLimits::default(),
        )
        .expect("pack should succeed");
        let target_setuid = root.join("out-setuid");
        std::fs::create_dir_all(&target_setuid).expect("target dir");
        qcg_service::package::unpack_qcg(
            &archive_setuid,
            &target_setuid,
            &qcg_service::PackageLimits::default(),
        )
        .expect("unpack should succeed");
        let restored = std::fs::metadata(target_setuid.join("bin/run.sh"))
            .expect("script metadata")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            restored & 0o7000,
            0,
            "setuid/setgid/sticky must never be restored: {restored:o}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn pack_records_sbom_modes_and_publishes_final_output_mode() {
        // E15: the SBOM records sanitized modes per file (verified on
        // unpack alongside hashes), and the staged archive is published
        // with its final mode already applied: no staging leftovers remain
        // and the output carries 0644 without any post-rename chmod.
        use std::os::unix::fs::PermissionsExt as _;
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-pack-modes-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let input = root.join("input");
        std::fs::create_dir_all(&input).expect("input dir");
        std::fs::write(
            input.join("qcg.toml"),
            "[generator]\nid = \"modes\"\nname = \"Modes\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        )
        .expect("manifest");
        std::fs::write(input.join("bin.sh"), "#!/bin/sh\n").expect("script");
        std::fs::set_permissions(input.join("bin.sh"), std::fs::Permissions::from_mode(0o755))
            .expect("script mode");
        std::fs::write(input.join("data.txt"), "data").expect("data");
        std::fs::set_permissions(
            input.join("data.txt"),
            std::fs::Permissions::from_mode(0o640),
        )
        .expect("data mode");
        let archive = root.join("pkg.qcg");
        package(&input, &archive, &qcg_service::PackageLimits::default())
            .expect("pack should succeed");
        let mode_of = |path: &Utf8Path| {
            std::fs::metadata(path)
                .expect("output metadata")
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(
            mode_of(&archive),
            0o644,
            "the published archive must carry the final mode"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .expect("root should be readable")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("qcg-part"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no staging temp may survive publication: {leftovers:?}"
        );
        // The SBOM inside the archive declares the sanitized modes.
        let file = std::fs::File::open(&archive).expect("archive should open");
        let mut reader = zip::ZipArchive::new(file).expect("archive should parse");
        let mut sbom = reader
            .by_name("QCG-SBOM.spdx.json")
            .expect("sbom should be listed");
        let mut sbom_bytes = Vec::new();
        std::io::Read::read_to_end(&mut sbom, &mut sbom_bytes).expect("sbom should read");
        let sbom: serde_json::Value =
            serde_json::from_slice(&sbom_bytes).expect("sbom should parse");
        let modes: std::collections::BTreeMap<String, u64> = sbom
            .get("files")
            .and_then(|files| files.as_array())
            .expect("sbom files should be listed")
            .iter()
            .map(|file| {
                (
                    file.get("fileName")
                        .and_then(|name| name.as_str())
                        .expect("fileName should be present")
                        .to_string(),
                    file.get("mode")
                        .and_then(|mode| mode.as_u64())
                        .expect("mode should be recorded"),
                )
            })
            .collect();
        assert_eq!(modes.get("bin.sh"), Some(&0o755));
        assert_eq!(modes.get("data.txt"), Some(&0o640));
    }
}

#[cfg(all(test, unix))]
mod swap_tests {
    use super::*;

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04). A removal failure warns instead of being silently
    /// ignored.
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(self.0.as_std_path()) {
                eprintln!("test temp cleanup failed for {}: {error}", self.0);
            }
        }
    }

    #[test]
    fn pack_refuses_a_planted_symlink_in_the_input() {
        // E15: pack reads the live input directory, so a planted symlink
        // anywhere in it must fail the pack instead of archiving
        // out-of-tree bytes.
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-pack-symlink-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        let _temp_guard = TempGuard(root.clone());
        let input = root.join("input");
        std::fs::create_dir_all(&input).expect("input dir");
        std::fs::write(
            input.join("qcg.toml"),
            "[generator]\nid = \"swapped\"\nname = \"Swapped\"\nversion = \"1.0.0\"\nqcg_version = \"^0.1\"\n",
        )
        .expect("manifest");
        std::fs::write(input.join("real.txt"), "real").expect("real file");
        std::os::unix::fs::symlink("real.txt", input.join("link.txt")).expect("planted symlink");
        let archive = root.join("pkg.qcg");
        let error = package(&input, &archive, &qcg_service::PackageLimits::default())
            .expect_err("a planted symlink must refuse the pack");
        assert!(
            error.to_string().contains("symbolic link"),
            "the refusal must name the symlink: {error}"
        );
    }
}
