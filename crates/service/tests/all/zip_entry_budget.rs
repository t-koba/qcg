//! Exercise the patched parser through its public API so normal workspace CI covers it.
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use zip::ZipArchive;
use zip::read::Config;
use zip::result::ZipError;

const ENTRY_LIMIT_ERROR: &str = "archive contains too many entries";

fn open(
    bytes: Vec<u8>,
    max_entries: Option<usize>,
) -> Result<ZipArchive<Cursor<Vec<u8>>>, ZipError> {
    ZipArchive::with_config(
        Config {
            max_entries,
            ..Config::default()
        },
        Cursor::new(bytes),
    )
}

fn assert_entry_limit(error: ZipError) {
    assert!(
        matches!(error, ZipError::UnsupportedArchive(ENTRY_LIMIT_ERROR)),
        "expected the inventory budget, got {error:?}"
    );
}

// Central-directory-only fixtures: construction ignores local file headers. The
// prefix makes the raw count eligible for the parser's initial capacity reserve.
fn inventory(names: &[&str], raw_count: u16, zip64: bool) -> Vec<u8> {
    let mut bytes = vec![0; usize::from(raw_count).max(64)];
    let start = bytes.len() as u64;
    for name in names {
        let mut header = [0_u8; 46];
        header[..4].copy_from_slice(&0x0201_4b50_u32.to_le_bytes());
        header[4..6].copy_from_slice(&0x0314_u16.to_le_bytes()); // Unix, ZIP 2.0
        header[6..8].copy_from_slice(&20_u16.to_le_bytes());
        header[28..30].copy_from_slice(&u16::try_from(name.len()).unwrap().to_le_bytes());
        header[38..42].copy_from_slice(&(0o100644_u32 << 16).to_le_bytes());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(name.as_bytes());
    }
    let size = bytes.len() as u64 - start;
    append_footer(&mut bytes, u64::from(raw_count), start, size, zip64);
    bytes
}

fn append_footer(bytes: &mut Vec<u8>, count: u64, start: u64, size: u64, zip64: bool) {
    if zip64 {
        let offset = start + size;
        bytes.extend_from_slice(&0x0606_4b50_u32.to_le_bytes());
        bytes.extend_from_slice(&44_u64.to_le_bytes()); // fixed ZIP64 EOCD size
        bytes.extend_from_slice(&45_u16.to_le_bytes());
        bytes.extend_from_slice(&45_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes()); // disk
        bytes.extend_from_slice(&0_u32.to_le_bytes()); // central directory disk
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&size.to_le_bytes());
        bytes.extend_from_slice(&start.to_le_bytes());
        bytes.extend_from_slice(&0x0706_4b50_u32.to_le_bytes()); // ZIP64 locator
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
    }
    bytes.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    let count32 = if zip64 {
        u16::MAX
    } else {
        u16::try_from(count).unwrap()
    };
    bytes.extend_from_slice(&count32.to_le_bytes());
    bytes.extend_from_slice(&count32.to_le_bytes());
    let size32 = if zip64 {
        u32::MAX
    } else {
        u32::try_from(size).unwrap()
    };
    let start32 = if zip64 {
        u32::MAX
    } else {
        u32::try_from(start).unwrap()
    };
    bytes.extend_from_slice(&size32.to_le_bytes());
    bytes.extend_from_slice(&start32.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes()); // no comment
}

#[test]
fn entry_budget_boundaries_and_none_for_zip32_and_zip64() {
    for zip64 in [false, true] {
        let bytes = inventory(&["first", "second"], 2, zip64);
        assert_eq!(open(bytes.clone(), Some(2)).unwrap().len(), 2);
        assert_eq!(open(bytes.clone(), Some(3)).unwrap().len(), 2);
        assert_eq!(open(bytes.clone(), None).unwrap().len(), 2);
        assert_eq!(
            ZipArchive::new(Cursor::new(bytes.clone())).unwrap().len(),
            2
        );
        assert_entry_limit(open(bytes.clone(), Some(1)).unwrap_err());
        assert_entry_limit(open(bytes, Some(0)).unwrap_err());
        assert!(open(inventory(&[], 0, zip64), Some(0)).unwrap().is_empty());
    }
}

#[test]
fn raw_duplicate_names_consume_the_budget_before_deduplication() {
    for zip64 in [false, true] {
        let bytes = inventory(&["same", "same"], 2, zip64);
        assert_entry_limit(open(bytes.clone(), Some(1)).unwrap_err());
        assert_eq!(open(bytes.clone(), Some(2)).unwrap().len(), 1);
        assert_eq!(open(bytes, None).unwrap().len(), 1);
    }
}

#[test]
fn none_does_not_install_a_default_entry_budget() {
    let names: Vec<_> = (0..1001).map(|i| format!("entry-{i}")).collect();
    let names: Vec<_> = names.iter().map(String::as_str).collect();
    for zip64 in [false, true] {
        let bytes = inventory(&names, 1001, zip64);
        assert_entry_limit(open(bytes.clone(), Some(1000)).unwrap_err());
        assert_eq!(open(bytes, None).unwrap().len(), 1001);
    }
}

#[test]
fn budget_precedes_inventory_reserve_and_malformed_entry_parsing() {
    // The count is <= directory_start, so the unbounded parser reserves 4096
    // slots before discovering that there is only one central-directory entry.
    let bytes = inventory(&["only-one"], 4096, false);
    assert_entry_limit(open(bytes.clone(), Some(1000)).unwrap_err());
    assert!(matches!(
        open(bytes, None),
        Err(ZipError::InvalidArchive(_))
    ));
}

#[test]
fn over_budget_footer_is_not_retried_as_a_malformed_candidate() {
    let mut bytes = inventory(&["earlier"], 1, false);
    bytes.extend_from_slice(&inventory(&["later", "later"], 2, false));
    assert_entry_limit(open(bytes, Some(1)).unwrap_err());
}

// A portable virtual zero prefix lets ZIP64's minimum-directory-size check
// accept a >u32 count without allocating a huge fixture or relying on sparse
// filesystem support. Bound reads too: a regression must fail, not scan GiB.
struct SparseFooter {
    position: u64,
    prefix_len: u64,
    footer: Vec<u8>,
    read_bytes: usize,
}

impl SparseFooter {
    fn new(count: u64) -> Self {
        let start = count.max(64);
        let size = count * 46;
        let mut footer = Vec::new();
        append_footer(&mut footer, count, start, size, true);
        Self {
            position: 0,
            prefix_len: start + size,
            footer,
            read_bytes: 0,
        }
    }

    fn len(&self) -> u64 {
        self.prefix_len + self.footer.len() as u64
    }
}

impl Read for SparseFooter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.read_bytes + buf.len() > 1024 * 1024 {
            return Err(io::Error::other("test reader exceeded its scan budget"));
        }
        let n = self
            .len()
            .saturating_sub(self.position)
            .min(buf.len() as u64) as usize;
        buf[..n].fill(0);
        let end = self.position + n as u64;
        if end > self.prefix_len && n != 0 {
            let source = self.position.saturating_sub(self.prefix_len) as usize;
            let destination = self.prefix_len.saturating_sub(self.position) as usize;
            buf[destination..n].copy_from_slice(&self.footer[source..source + n - destination]);
        }
        self.position = end;
        self.read_bytes += n;
        Ok(n)
    }
}

impl Seek for SparseFooter {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::End(n) => i128::from(self.len()) + i128::from(n),
            SeekFrom::Current(n) => i128::from(self.position) + i128::from(n),
        };
        self.position = u64::try_from(position)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid test seek"))?;
        Ok(self.position)
    }
}

#[test]
fn zip64_raw_count_is_not_truncated_before_budget_check() {
    // A narrowing `as usize` would turn this into 1 on a 32-bit target.
    // On 64-bit targets the count would hit the allocation-size guard even
    // before parsing: the entry budget must take precedence over that too.
    let error = ZipArchive::with_config(
        Config {
            max_entries: Some(1),
            ..Config::default()
        },
        SparseFooter::new((1_u64 << 56) + 1),
    )
    .err()
    .expect("raw ZIP64 count must exceed the budget");
    assert_entry_limit(error);
}

#[test]
fn unbounded_zip64_count_must_still_fit_inventory_capacity() {
    let error = ZipArchive::new(SparseFooter::new((1_u64 << 56) + 1))
        .err()
        .expect("oversized ZIP64 count must not wrap or allocate");
    assert!(matches!(
        error,
        ZipError::UnsupportedArchive("Oversized central directory")
    ));
}

#[test]
fn unpack_uses_the_configured_budget_before_building_inventory() {
    use camino::Utf8PathBuf;
    use service::{PackageLimits, package::unpack_package};

    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-zip-budget-{}", uuid::Uuid::now_v7())),
    )
    .unwrap();
    struct Cleanup(Utf8PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0) {
                eprintln!("test cleanup failed: {error}");
            }
        }
    }
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let archive = root.join("inventory.pkg");
    let target = root.join("out");
    std::fs::create_dir_all(&target).unwrap();
    // Eight declared entries, one present: the default (1000), None, and
    // the exact boundary (8) would all reach the malformed inventory.
    std::fs::write(&archive, inventory(&["one"], 8, false)).unwrap();
    for max_entries in [Some(7), Some(8), None] {
        let error = unpack_package(
            &archive,
            &target,
            &PackageLimits {
                max_entries,
                ..PackageLimits::default()
            },
        )
        .expect_err("synthetic inventory must fail");
        assert_eq!(
            error.to_string().contains(ENTRY_LIMIT_ERROR),
            max_entries == Some(7),
            "{error}"
        );
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    }
}
