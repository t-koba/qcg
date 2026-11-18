use std::fs::{File, Metadata};
use std::io;

pub(crate) type FileIdentity = (u64, u64);

/// Returns the identity of an open file, using metadata from that same handle on Unix.
pub(crate) fn file_identity(file: &File, metadata: &Metadata) -> io::Result<Option<FileIdentity>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let _ = file;
        let inode = metadata.ino();
        Ok((inode != 0).then_some((metadata.dev(), inode)))
    }
    #[cfg(windows)]
    {
        let _ = metadata;
        Ok(windows_file_info(file)?.0)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, metadata);
        Ok(None)
    }
}

#[cfg(windows)]
pub(crate) fn windows_file_info(file: &File) -> io::Result<(Option<FileIdentity>, u32)> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: the handle is live and info is writable storage of the required type.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: success above initialized all fields.
    let info = unsafe { info.assume_init() };
    let index = (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow);
    Ok((
        (index != 0).then_some((u64::from(info.dwVolumeSerialNumber), index)),
        info.nNumberOfLinks,
    ))
}
