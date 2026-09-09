use qcg_engine::StepError;
use sha2::{Digest, Sha256};
use std::io::Read as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::unix_mode::apply_unix_mode;

pub(crate) async fn bounded_transform_text(
    path: &camino::Utf8Path,
    limit: Option<usize>,
) -> Result<String, String> {
    let bytes = bounded_file_bytes(path, limit).await?;
    String::from_utf8(bytes).map_err(|error| format!("transform input is not valid UTF-8: {error}"))
}

pub(crate) async fn ensure_bounded_file_tree(
    path: &camino::Utf8Path,
    limit: Option<usize>,
    count_limit: Option<usize>,
) -> Result<(), String> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut total = 0_usize;
        let mut entries = 0_usize;
        if path.is_file() {
            let size = usize::try_from(
                std::fs::metadata(&path)
                    .map_err(|error| format!("failed to inspect file input: {error}"))?
                    .len(),
            )
            .map_err(|_| "file input size does not fit in usize".to_owned())?;
            if limit.is_some_and(|limit| size > limit) {
                return Err(format!(
                    "file input exceeds {} bytes",
                    limit.unwrap_or(usize::MAX)
                ));
            }
            return Ok(());
        }
        if !path.is_dir() {
            return Err(format!("file input `{path}` is not a file or directory"));
        }
        for entry in qcg_fs::WalkDir::new(&path) {
            let entry = entry.map_err(|error| format!("failed to inspect file input: {error}"))?;
            entries = entries
                .checked_add(1)
                .ok_or_else(|| "file input entry count overflowed".to_owned())?;
            if count_limit.is_some_and(|limit| entries > limit) {
                return Err(format!(
                    "file input contains more than {} entries",
                    count_limit.unwrap_or(usize::MAX)
                ));
            }
            if !entry.file_type().is_file() {
                continue;
            }
            let size = usize::try_from(
                entry
                    .metadata()
                    .map_err(|error| format!("failed to inspect file input: {error}"))?
                    .len(),
            )
            .map_err(|_| "file input size does not fit in usize".to_owned())?;
            total = total
                .checked_add(size)
                .ok_or_else(|| "file input size overflowed".to_owned())?;
            if limit.is_some_and(|limit| total > limit) {
                return Err(format!(
                    "file input exceeds {} bytes",
                    limit.unwrap_or(usize::MAX)
                ));
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("file input bound worker failed: {error}"))??;
    Ok(())
}

pub(crate) fn bounded_sha256_file(
    path: &camino::Utf8Path,
    limit: Option<usize>,
) -> Result<String, String> {
    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut total = 0_usize;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read)
            .ok_or_else(|| "file input size overflowed".to_owned())?;
        if limit.is_some_and(|limit| total > limit) {
            return Err(format!(
                "file input exceeds {} bytes",
                limit.unwrap_or(usize::MAX)
            ));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) async fn bounded_file_bytes(
    path: &camino::Utf8Path,
    limit: Option<usize>,
) -> Result<Vec<u8>, String> {
    let Some(limit) = limit else {
        return tokio::fs::read(path)
            .await
            .map_err(|error| error.to_string());
    };
    let mut bytes = Vec::new();
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|error| error.to_string())?;
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    if bytes.len() > limit {
        return Err(format!("source exceeds {limit} bytes"));
    }
    Ok(bytes)
}

pub(crate) fn atomic_temp_path(path: &camino::Utf8Path) -> camino::Utf8PathBuf {
    let file_name = path.file_name().unwrap_or("output");
    path.with_file_name(format!(
        ".{file_name}.qcg-part-{}",
        uuid::Uuid::now_v7().as_simple()
    ))
}

pub(crate) async fn write_atomic(
    path: &camino::Utf8Path,
    bytes: &[u8],
    unix_mode: Option<u32>,
) -> Result<(), std::io::Error> {
    let temporary = atomic_temp_path(path);
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        apply_unix_mode(&temporary, unix_mode).map_err(std::io::Error::other)?;
        atomic_replace(&temporary, path).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

pub(crate) async fn write_transform_output(
    node_id: &str,
    path: &camino::Utf8Path,
    bytes: &[u8],
    limit: Option<usize>,
) -> Result<(), StepError> {
    if limit.is_some_and(|limit| bytes.len() > limit) {
        return Err(StepError::failed(
            node_id,
            format!(
                "transform output exceeds runtime.output_file_limit_bytes ({} bytes)",
                limit.unwrap_or(usize::MAX)
            ),
        ));
    }
    write_atomic(path, bytes, None)
        .await
        .map_err(|error| StepError::failed(node_id, error.to_string()))
}

pub(crate) async fn atomic_replace(
    temporary: &camino::Utf8Path,
    target: &camino::Utf8Path,
) -> Result<(), std::io::Error> {
    #[cfg(not(windows))]
    {
        tokio::fs::rename(temporary, target).await
    }
    #[cfg(windows)]
    {
        let temporary = temporary.to_owned();
        let target = target.to_owned();
        tokio::task::spawn_blocking(move || {
            use std::iter::once;
            use std::os::windows::ffi::OsStrExt as _;
            use windows_sys::Win32::Storage::FileSystem::{
                MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
            };
            let source = temporary
                .as_std_path()
                .as_os_str()
                .encode_wide()
                .chain(once(0))
                .collect::<Vec<_>>();
            let destination = target
                .as_std_path()
                .as_os_str()
                .encode_wide()
                .chain(once(0))
                .collect::<Vec<_>>();
            // SAFETY: both paths are NUL-terminated UTF-16 strings owned for this call.
            let moved = unsafe {
                MoveFileExW(
                    source.as_ptr(),
                    destination.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };
            if moved == 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
        .await
        .map_err(std::io::Error::other)?
    }
}
