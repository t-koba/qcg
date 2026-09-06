//! Bounded synchronous I/O: reads and hashes with an explicit optional cap.
//! `None` means no mechanistic limit; the caller sets a max only when wanted.

use camino::Utf8Path;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read as _, Write};

/// Reads a file, enforcing an explicit size cap only when set.
pub fn read_bounded(path: &Utf8Path, max_bytes: Option<usize>) -> std::io::Result<Vec<u8>> {
    let Some(max_bytes) = max_bytes else {
        return std::fs::read(path);
    };
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(std::io::Error::other(format!(
            "file `{path}` exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}

/// Streams a file through SHA-256, enforcing an explicit size cap only when
/// set. Returns the hex digest and byte count.
pub fn hash_file_sha256(path: &Utf8Path, max_bytes: Option<u64>) -> std::io::Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("file byte count overflowed"))?;
        if max_bytes.is_some_and(|limit| total > limit) {
            return Err(std::io::Error::other(format!(
                "file `{path}` exceeds {} bytes",
                max_bytes.unwrap_or(u64::MAX)
            )));
        }
        digest.update(&buffer[..read]);
    }
    Ok((hex::encode(digest.finalize()), total))
}

/// Writes bytes while enforcing an explicit size cap only when set.
pub fn write_bounded<W: Write>(
    mut writer: W,
    bytes: &[u8],
    max_bytes: Option<usize>,
    resource: &str,
) -> std::io::Result<()> {
    if max_bytes.is_some_and(|limit| bytes.len() > limit) {
        return Err(std::io::Error::other(format!(
            "{resource} exceeds {} bytes",
            max_bytes.unwrap_or(usize::MAX)
        )));
    }
    writer.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    fn temp_file(name: &str, bytes: &[u8]) -> Utf8PathBuf {
        let path = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-policy-test-{name}-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        std::fs::write(&path, bytes).expect("fixture should be writable");
        path
    }

    #[test]
    fn read_bounded_honors_an_explicit_cap() {
        let path = temp_file("read", b"hello");
        assert_eq!(read_bounded(&path, None).expect("uncapped read"), b"hello");
        assert_eq!(read_bounded(&path, Some(5)).expect("exact cap"), b"hello");
        let error = read_bounded(&path, Some(4)).expect_err("over-cap read must fail");
        assert!(error.to_string().contains("exceeds"), "{error}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_bounded_reports_missing_files() {
        let missing =
            Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-policy-test-missing"))
                .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_file(&missing);
        read_bounded(&missing, None).expect_err("missing file must fail");
    }

    #[test]
    fn hash_reports_digest_and_count_with_optional_cap() {
        let path = temp_file("hash", b"abc");
        let (hex, count) = hash_file_sha256(&path, None).expect("hash should succeed");
        assert_eq!(count, 3);
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let (hex_capped, count_capped) =
            hash_file_sha256(&path, Some(3)).expect("exact cap should pass");
        assert_eq!((hex_capped.as_str(), count_capped), (hex.as_str(), 3));
        let error = hash_file_sha256(&path, Some(2)).expect_err("over-cap hash must fail");
        assert!(error.to_string().contains("exceeds"), "{error}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_bounded_enforces_the_cap() {
        let mut sink = Vec::new();
        write_bounded(&mut sink, b"hello", Some(5), "test").expect("exact cap should pass");
        assert_eq!(sink, b"hello");
        let mut sink = Vec::new();
        let error = write_bounded(&mut sink, b"hello!", Some(5), "test")
            .expect_err("over-cap write must fail");
        assert!(error.to_string().contains("exceeds"), "{error}");
        let mut sink = Vec::new();
        write_bounded(&mut sink, b"hello!", None, "test").expect("uncapped write should pass");
    }
}
