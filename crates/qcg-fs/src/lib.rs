//! Bounded synchronous file I/O: reads, hashes, and atomic writes with an
//! explicit optional cap. `None` means no mechanistic limit; the caller sets
//! a max only when wanted.

use camino::{Utf8Path, Utf8PathBuf};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read as _, Write};
use std::sync::atomic::{AtomicU64, Ordering};

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

/// A single directory-tree entry produced by [`WalkDir`].
#[derive(Debug)]
pub struct WalkEntry {
    path: Utf8PathBuf,
    file_type: std::fs::FileType,
    depth: usize,
}

impl WalkEntry {
    /// Full path of the entry. The walk root itself is yielded at depth 0.
    pub fn path(&self) -> &Utf8Path {
        &self.path
    }

    /// Entry type observed without following symbolic links.
    pub fn file_type(&self) -> std::fs::FileType {
        self.file_type
    }

    /// Nesting depth relative to the walk root, which has depth 0.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Metadata of the entry, following symbolic links like [`std::fs::metadata`].
    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        std::fs::metadata(&self.path)
    }
}

struct WalkFrame {
    child_depth: usize,
    read: std::fs::ReadDir,
}

/// Lazily walks a directory tree top-down without following symbolic links.
/// Only a small [`walkdir`](https://crates.io/crates/walkdir)-compatible
/// surface is provided: construction from a root, [`WalkDir::min_depth`],
/// and iteration over `Result<WalkEntry, io::Error>`. Sibling order follows
/// the operating system directory order and is not sorted.
pub struct WalkDir {
    root: Option<Result<WalkEntry, std::io::Error>>,
    stack: Vec<WalkFrame>,
    min_depth: usize,
}

impl WalkDir {
    /// Starts a walk at `root`. A missing root yields a single iteration
    /// error instead of failing eagerly.
    pub fn new(root: &Utf8Path) -> Self {
        let root = match std::fs::symlink_metadata(root) {
            Ok(metadata) => Ok(WalkEntry {
                path: root.to_path_buf(),
                file_type: metadata.file_type(),
                depth: 0,
            }),
            Err(error) => Err(error),
        };
        Self {
            root: Some(root),
            stack: Vec::new(),
            min_depth: 0,
        }
    }

    /// Skips entries shallower than `depth`. The root has depth 0, so
    /// `min_depth(1)` skips the root itself while still descending into it.
    pub fn min_depth(mut self, depth: usize) -> Self {
        self.min_depth = depth;
        self
    }

    fn descend(&mut self, entry: &WalkEntry) -> std::io::Result<()> {
        if entry.file_type.is_dir() {
            let read = std::fs::read_dir(&entry.path)?;
            self.stack.push(WalkFrame {
                child_depth: entry.depth + 1,
                read,
            });
        }
        Ok(())
    }
}

impl Iterator for WalkDir {
    type Item = Result<WalkEntry, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(root) = self.root.take() {
            match root {
                Err(error) => return Some(Err(error)),
                Ok(entry) => {
                    if let Err(error) = self.descend(&entry) {
                        return Some(Err(error));
                    }
                    if entry.depth >= self.min_depth {
                        return Some(Ok(entry));
                    }
                }
            }
        }
        loop {
            let mut frame = self.stack.pop()?;
            let dir_entry = match frame.read.next() {
                None => continue,
                Some(Err(error)) => {
                    self.stack.push(frame);
                    return Some(Err(error));
                }
                Some(Ok(dir_entry)) => dir_entry,
            };
            let path = match Utf8PathBuf::from_path_buf(dir_entry.path()) {
                Ok(path) => path,
                Err(path) => {
                    self.stack.push(frame);
                    return Some(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("path is not UTF-8: {}", path.display()),
                    )));
                }
            };
            let file_type = match dir_entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    self.stack.push(frame);
                    return Some(Err(error));
                }
            };
            let entry = WalkEntry {
                path,
                file_type,
                depth: frame.child_depth,
            };
            self.stack.push(frame);
            if let Err(error) = self.descend(&entry) {
                return Some(Err(error));
            }
            if entry.depth >= self.min_depth {
                return Some(Ok(entry));
            }
        }
    }
}

/// Creates `path` atomically: content produced by `write` is staged in a
/// uniquely named sibling file with owner-only permissions, synced, and then
/// renamed over the destination. A failed attempt removes its staging file
/// instead of littering the directory.
pub fn write_file_atomic(
    path: &Utf8Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other(format!("`{path}` has no parent directory")))?;
    static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let file_name = path.file_name().unwrap_or("file");
    for _ in 0..100 {
        let nonce = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let staging = parent.join(format!(".{file_name}.{pid}.{nonce}.tmp"));
        let mut options = File::options();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = match options.open(&staging) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let outcome = write(&mut file).and_then(|()| file.sync_all());
        drop(file);
        match outcome {
            Ok(()) => return std::fs::rename(&staging, path),
            Err(error) => {
                let _ = std::fs::remove_file(&staging);
                return Err(error);
            }
        }
    }
    Err(std::io::Error::other(format!(
        "failed to stage atomic write for `{path}`"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    fn temp_file(name: &str, bytes: &[u8]) -> Utf8PathBuf {
        let path = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-fs-test-{name}-{}", std::process::id())),
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
        let missing = Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-fs-test-missing"))
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

    #[test]
    fn write_file_atomic_replaces_destination_content() {
        let dir = test_dir("atomic-ok");
        let path = dir.join("outputs.json");
        std::fs::write(&path, b"old").expect("fixture should be writable");
        write_file_atomic(&path, |file| {
            use std::io::Write as _;
            file.write_all(b"new")
        })
        .expect("atomic write should succeed");
        assert_eq!(
            std::fs::read(&path).expect("result should be readable"),
            b"new"
        );
        assert!(
            staging_files(&dir).is_empty(),
            "no staging files should remain"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_file_atomic_removes_staging_on_failure() {
        let dir = test_dir("atomic-fail");
        let path = dir.join("outputs.json");
        let error = write_file_atomic(&path, |_file| {
            Err(std::io::Error::other("synthetic write failure"))
        })
        .expect_err("failing write must fail");
        assert!(
            error.to_string().contains("synthetic write failure"),
            "{error}"
        );
        assert!(
            !path.exists(),
            "destination must not appear after a failed write"
        );
        assert!(
            staging_files(&dir).is_empty(),
            "failed staging files must be removed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_file_atomic_rejects_parentless_paths() {
        let error = write_file_atomic(Utf8Path::new(""), |_file| Ok(()))
            .expect_err("parentless path must fail");
        assert!(error.to_string().contains("no parent directory"), "{error}");
    }

    fn test_dir(name: &str) -> Utf8PathBuf {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-fs-test-{name}-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test directory should be creatable");
        dir
    }

    fn staging_files(dir: &Utf8Path) -> Vec<Utf8PathBuf> {
        std::fs::read_dir(dir)
            .expect("test directory should be readable")
            .filter_map(|entry| entry.ok())
            .map(|entry| {
                Utf8PathBuf::from_path_buf(entry.path()).expect("temporary path must be UTF-8")
            })
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"))
            })
            .collect()
    }

    #[test]
    fn walk_dir_yields_tree_top_down_with_depths() {
        let dir = test_dir("walk-basic");
        std::fs::create_dir_all(dir.join("sub/nested")).expect("fixture dirs should be created");
        std::fs::write(dir.join("top.txt"), b"top").expect("fixture should be writable");
        std::fs::write(dir.join("sub/mid.txt"), b"mid").expect("fixture should be writable");
        std::fs::write(dir.join("sub/nested/deep.txt"), b"deep")
            .expect("fixture should be writable");

        let entries: Vec<WalkEntry> = WalkDir::new(&dir)
            .collect::<Result<_, _>>()
            .expect("walk should succeed");
        // Root plus two directories plus three files.
        assert_eq!(entries.len(), 6);
        assert_eq!(entries[0].depth(), 0);
        assert!(entries[0].file_type().is_dir());
        let mut by_path: Vec<(String, usize, bool)> = entries
            .iter()
            .map(|entry| {
                (
                    entry
                        .path()
                        .strip_prefix(&dir)
                        .expect("entry lives under root")
                        .as_str()
                        .replace(std::path::MAIN_SEPARATOR, "/"),
                    entry.depth(),
                    entry.file_type().is_dir(),
                )
            })
            .collect();
        by_path.sort();
        assert_eq!(
            by_path,
            vec![
                ("".to_string(), 0, true),
                ("sub".to_string(), 1, true),
                ("sub/mid.txt".to_string(), 2, false),
                ("sub/nested".to_string(), 2, true),
                ("sub/nested/deep.txt".to_string(), 3, false),
                ("top.txt".to_string(), 1, false),
            ]
        );
        // Parents precede their children.
        let position = |name: &str| by_path.iter().position(|entry| entry.0 == name);
        assert!(position("sub") < position("sub/mid.txt"));
        assert!(position("sub/nested") < position("sub/nested/deep.txt"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn walk_dir_min_depth_skips_root_but_descends() {
        let dir = test_dir("walk-min-depth");
        std::fs::write(dir.join("top.txt"), b"top").expect("fixture should be writable");

        let entries: Vec<WalkEntry> = WalkDir::new(&dir)
            .min_depth(1)
            .collect::<Result<_, _>>()
            .expect("walk should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].depth(), 1);
        assert_eq!(
            entries[0]
                .path()
                .strip_prefix(&dir)
                .expect("entry lives under root")
                .as_str(),
            "top.txt"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn walk_dir_reports_missing_root_as_iteration_error() {
        let missing = test_dir("walk-missing");
        let _ = std::fs::remove_dir_all(&missing);
        let mut walk = WalkDir::new(&missing);
        assert!(
            walk.next()
                .expect("missing root must yield an error")
                .is_err()
        );
        assert!(walk.next().is_none(), "walk must end after the error");
    }

    #[cfg(unix)]
    #[test]
    fn walk_dir_never_follows_symbolic_links() {
        use std::os::unix::fs::symlink;

        let dir = test_dir("walk-symlink");
        std::fs::create_dir_all(dir.join("real")).expect("fixture dir should be created");
        std::fs::write(dir.join("real/file.txt"), b"data").expect("fixture should be writable");
        symlink(dir.join("real"), dir.join("linked")).expect("symlink should be created");
        symlink("nowhere-missing", dir.join("dangling")).expect("symlink should be created");

        let entries: Vec<WalkEntry> = WalkDir::new(&dir)
            .min_depth(1)
            .collect::<Result<_, _>>()
            .expect("walk should succeed");
        let names: Vec<String> = entries
            .iter()
            .map(|entry| {
                entry
                    .path()
                    .strip_prefix(&dir)
                    .expect("entry lives under root")
                    .as_str()
                    .to_string()
            })
            .collect();
        assert!(names.contains(&"linked".to_string()));
        assert!(names.contains(&"dangling".to_string()));
        // The linked tree is not descended into: no nested copy appears.
        assert!(
            !names.iter().any(|name| name.starts_with("linked/")),
            "symlinked directories must not be traversed: {names:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
