//! Exercise the production rawzip reader through unpack_package, not a
//! separate parser preflight. Fixtures stay small even for huge ZIP64 hints.
use camino::Utf8PathBuf;
use service::{PackageLimits, package::unpack_package};

const ENTRY_LIMIT_ERROR: &str = "archive contains too many entries";

fn unpack(bytes: &[u8], max_entries: Option<usize>) -> (String, usize) {
    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-zip-budget-{}", uuid::Uuid::now_v7())),
    )
    .unwrap();
    struct Cleanup(Utf8PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    std::fs::create_dir_all(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let archive = root.join("inventory.pkg");
    let target = root.join("out");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(&archive, bytes).unwrap();
    // These fixtures deliberately omit supply-chain metadata. Reaching the
    // missing-SBOM error means the entire ZIP was accepted and extracted.
    let error = unpack_package(
        &archive,
        &target,
        &PackageLimits {
            max_entries,
            ..PackageLimits::default()
        },
    )
    .expect_err("fixture has no SBOM")
    .to_string();
    (error, std::fs::read_dir(target).unwrap().count())
}

fn assert_accepted(bytes: &[u8], limit: Option<usize>, files: usize) {
    let (error, extracted) = unpack(bytes, limit);
    assert!(
        error.contains("package is missing SBOM.spdx.json"),
        "{error}"
    );
    assert_eq!(extracted, files);
}

fn assert_entry_limit(bytes: &[u8], limit: usize) {
    let (error, extracted) = unpack(bytes, Some(limit));
    assert!(error.contains(ENTRY_LIMIT_ERROR), "{error}");
    assert_eq!(extracted, 0, "budget must fail before extraction");
}

fn assert_invalid_zip(bytes: &[u8], limit: Option<usize>) {
    let (error, extracted) = unpack(bytes, limit);
    assert!(error.contains("invalid package ZIP"), "{error}");
    assert_eq!(extracted, 0, "invalid ZIP must fail before extraction");
}

// Empty stored files with explicit Unix modes; the footer count is independent
// of the actual records, to exercise lying counts and raw duplicate accounting.
fn inventory(names: &[&str], raw_count: u64, zip64: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for name in names {
        offsets.push(u32::try_from(bytes.len()).unwrap());
        let mut local = [0_u8; 30];
        local[..4].copy_from_slice(&0x0403_4b50_u32.to_le_bytes());
        local[4..6].copy_from_slice(&20_u16.to_le_bytes());
        local[26..28].copy_from_slice(&u16::try_from(name.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(&local);
        bytes.extend_from_slice(name.as_bytes());
    }
    let start = bytes.len() as u64;
    for (name, offset) in names.iter().zip(offsets) {
        let mut header = [0_u8; 46];
        header[..4].copy_from_slice(&0x0201_4b50_u32.to_le_bytes());
        header[4..6].copy_from_slice(&0x0314_u16.to_le_bytes());
        header[6..8].copy_from_slice(&20_u16.to_le_bytes());
        header[28..30].copy_from_slice(&u16::try_from(name.len()).unwrap().to_le_bytes());
        header[38..42].copy_from_slice(&(0o100644_u32 << 16).to_le_bytes());
        header[42..46].copy_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(name.as_bytes());
    }
    let size = bytes.len() as u64 - start;
    append_footer(&mut bytes, raw_count, start, size, zip64);
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
        for limit in [Some(2), Some(3), None] {
            assert_accepted(&bytes, limit, 2);
        }
        assert_entry_limit(&bytes, 1);
        assert_entry_limit(&bytes, 0);

        let empty = inventory(&[], 0, zip64);
        if zip64 {
            // rawzip 0.5.2 treats a ZIP64 EOCD at offset 0 as a central
            // entry; this metadata-free generic ZIP is not an installable package.
            assert_invalid_zip(&empty, Some(0));
            assert_invalid_zip(&empty, None);
        } else {
            assert_accepted(&empty, Some(0), 0);
        }
    }
}

#[test]
fn raw_duplicate_names_consume_budget_before_duplicate_rejection() {
    for zip64 in [false, true] {
        let bytes = inventory(&["same", "same"], 2, zip64);
        assert_entry_limit(&bytes, 1);
        for limit in [Some(2), None] {
            let (error, _) = unpack(&bytes, limit);
            assert!(error.contains("duplicate path `same`"), "{error}");
        }
        // The actual counter must include duplicates even when the hint lies.
        assert_entry_limit(&inventory(&["same", "same"], 1, zip64), 1);
    }
}

#[test]
fn none_does_not_install_a_default_entry_budget() {
    let names: Vec<_> = (0..1001).map(|i| format!("entry-{i}")).collect();
    let names: Vec<_> = names.iter().map(String::as_str).collect();
    for zip64 in [false, true] {
        let bytes = inventory(&names, 1001, zip64);
        assert_entry_limit(&bytes, 1000);
        assert_accepted(&bytes, None, 1001);
    }
}

#[test]
fn lying_counts_fail_before_extraction_even_when_unbounded() {
    for zip64 in [false, true] {
        for (names, hint) in [
            (&["one"][..], 8),
            (&["one", "two"][..], 1),
            (&["one"][..], 0),
        ] {
            let bytes = inventory(names, hint, zip64);
            for limit in [Some(8), None] {
                let (error, extracted) = unpack(&bytes, limit);
                assert!(error.contains("entry count mismatch"), "{error}");
                assert_eq!(extracted, 0);
            }
        }
        assert_entry_limit(&inventory(&["one", "two"], 1, zip64), 1);
        assert_entry_limit(&inventory(&["one"], 8, zip64), 7);
    }
}

#[test]
fn hint_gate_precedes_malformed_central_record_parsing() {
    for zip64 in [false, true] {
        let mut bytes = inventory(&["only-one"], 4096, zip64);
        let central = 30 + "only-one".len();
        bytes[central] = 0; // corrupt the central header, not the footer
        assert_entry_limit(&bytes, 1000);
        let (error, extracted) = unpack(&bytes, None);
        assert!(error.contains("invalid package ZIP"), "{error}");
        assert_eq!(extracted, 0);
    }
}

#[test]
fn over_budget_footer_is_not_retried_as_a_malformed_candidate() {
    let mut bytes = inventory(&["earlier"], 1, false);
    bytes.extend_from_slice(&inventory(&["later", "later"], 2, false));
    assert_entry_limit(&bytes, 1);
}

#[test]
fn zip64_raw_count_is_not_narrowed_or_used_as_inventory_capacity() {
    // rawzip needs no virtual giant directory to expose this hint. The full
    // input is <200 bytes; neither bounded nor unbounded reads may allocate
    // based on the count, truncate it to usize, or iterate to that count.
    for count in [u64::from(u32::MAX) + 2, (1_u64 << 56) + 1, u64::MAX] {
        let bytes = inventory(&["one"], count, true);
        assert!(bytes.len() < 200);
        assert_entry_limit(&bytes, 1);
        let (error, extracted) = unpack(&bytes, None);
        assert!(error.contains("entry count mismatch"), "{error}");
        assert_eq!(extracted, 0);
    }
}
