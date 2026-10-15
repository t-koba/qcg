# qcg local patch: ZIP inventory entry budget

## Provenance and license

This directory contains the complete published **zip 8.6.0** crate from crates.io,
with changes limited to the three Rust source files listed below and this note.
The original MIT license and copyright are preserved in `LICENSE`.

- Published `.crate` SHA-256 (the original Cargo.lock checksum):
  `2d04a6b5381502aa6087c94c669499eb1602eb9c5e8198e534de571f7154809b`
- Upstream repository: <https://github.com/zip-rs/zip2>
- Published VCS revision: `771dfc534d2614158af5497ea3dff4d4208d7db1`
  (also recorded in `.cargo_vcs_info.json`).

The workspace uses `[patch.crates-io]` to select this local source. No unpublished
ref or network source is needed. Keep the other published files unchanged when
maintaining this patch; remove the patch when a released upstream API provides
an equivalent pre-inventory budget.

## Source changes

- `src/read/config.rs`: add `Config::max_entries: Option<usize>`; remove the
  now-inapplicable single-field `repr(transparent)`. The default is `None`, so
  existing unconfigured readers remain unbounded. A limit counts **raw central
  directory records**, including duplicate names, not the deduplicated
  `ZipArchive::len()` result.
- `src/read.rs`: retain the footer's entry count as `u64`, including ZIP64 counts.
- `src/read/zip_archive.rs`: check the configured budget after footer discovery
  and validation, before inventory reservation or entry parsing. A budget
  failure returns `UnsupportedArchive("archive contains too many entries")`
  immediately; unlike malformed-footer errors it is not retried against an
  older footer. Convert the count to `usize` with `try_from` before use, retaining
  the existing oversized-inventory guard. Compare directory offsets without
  narrowing them. Initialize the added configuration field for finalized writers
  using its default.

This is an inventory-entry capability, not a byte, filename, footer-metadata, or
archive-scanning budget. It adds no default compressed-byte restriction and no
second ZIP parser. qcg passes its existing `PackageLimits::max_entries` through
and retains its post-construction count check.

## Regression coverage

The published crate excludes upstream test fixtures, so the local patch's public
API regressions live in the consuming workspace and run with ordinary CI:

```sh
cargo test -p service --test all zip_entry_budget --locked --offline
```

Coverage includes ZIP32/ZIP64 boundaries (including zero), `None`, raw duplicate
names, a malformed inventory that would otherwise reserve and parse entries,
a synthetic ZIP64 count above 32-bit range and inventory capacity, terminal
budget errors, and qcg unpack's configured limit. The huge ZIP64 fixture is a
bounded in-memory `Read + Seek` simulation, not a large allocation, sparse file,
or platform-specific RSS assertion.
