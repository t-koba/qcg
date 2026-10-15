//! Read Config

/// Configuration for reading ZIP archives.
#[derive(Debug, Default, Clone, Copy)]
pub struct Config {
    /// An offset into the reader to use to find the start of the archive.
    pub archive_offset: ArchiveOffset,
    /// Maximum raw central-directory entry count, checked before inventory allocation or parsing.
    ///
    /// Duplicate names count separately, even though [`crate::ZipArchive::len`] deduplicates them.
    /// `None` (the default) leaves the entry count unbounded.
    pub max_entries: Option<usize>,
}

/// The offset of the start of the archive from the beginning of the reader.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveOffset {
    /// Try to detect the archive offset automatically.
    ///
    /// This will look at the central directory specified by `FromCentralDirectory` for a header.
    /// If missing, this will behave as if `None` were specified.
    #[default]
    Detect,
    /// Use the central directory length and offset to determine the start of the archive.
    #[deprecated(since = "2.3.0", note = "use `Detect` instead")]
    FromCentralDirectory,
    /// Specify a fixed archive offset.
    Known(u64),
}
