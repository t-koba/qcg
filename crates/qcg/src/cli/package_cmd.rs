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
            anyhow::bail!(
                "package input contains too many entries: exceeds {}",
                limits.max_entries.unwrap_or(usize::MAX)
            );
        }
        let metadata = entry.metadata()?;
        if entry.file_type().is_dir() {
            directories.push((name, metadata));
            continue;
        }
        if !entry.file_type().is_file() {
            anyhow::bail!("package input contains an unsupported entry: {path}");
        }
        let bytes = metadata.len();
        total_bytes = total_bytes
            .checked_add(bytes)
            .context("package input size overflowed")?;
        if limits.max_bytes.is_some_and(|limit| total_bytes > limit) {
            anyhow::bail!(
                "package input exceeds {} bytes",
                limits.max_bytes.unwrap_or(u64::MAX)
            );
        }
        let sha256 = sha256_file(&path)?;
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
    let file = File::create(output)?;
    let mut zip = zip::ZipWriter::new(file);
    for (name, metadata) in &directories {
        zip.add_directory(format!("{name}/"), package_zip_options(metadata, true)?)?;
    }
    for entry in &entries {
        zip.start_file(
            &entry.archive_path,
            package_zip_options(&entry.metadata, false)?,
        )?;
        let (bytes, sha256) = copy_file_with_sha256(&entry.source_path, &mut zip)?;
        if bytes != entry.bytes || sha256 != entry.sha256 {
            anyhow::bail!(
                "package input changed while being archived: {}",
                entry.source_path
            );
        }
    }
    let generated_options = generated_package_zip_options(SystemTime::now())?;
    for (name, bytes) in [(SBOM_PATH, sbom), (PROVENANCE_PATH, provenance)] {
        zip.start_file(name, generated_options)?;
        zip.write_all(&bytes)?;
    }
    zip.finish()?;
    Ok(())
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
        options = options.unix_permissions(metadata.permissions().mode());
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

pub(crate) fn sha256_file(path: &Utf8Path) -> Result<String> {
    Ok(qcg_fs::hash_file_sha256(path, None)?.0)
}

fn copy_file_with_sha256<W: Write>(path: &Utf8Path, writer: &mut W) -> Result<(u64, String)> {
    let mut file = File::open(path)?;
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
    }
    Ok((bytes, hex::encode(digest.finalize())))
}
