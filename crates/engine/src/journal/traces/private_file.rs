//! Owner-only access for private continuation evidence; never use ambient defaults.
use camino::Utf8Path;
use std::fs::File;
use std::io;

#[derive(Clone, Copy)]
pub(super) enum Access {
    Read,
    Append,
}

#[cfg(unix)]
pub(super) fn open(path: &Utf8Path, access: Access) -> io::Result<File> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    match access {
        Access::Read => {
            options.read(true);
        }
        Access::Append => {
            options.create(true).append(true).mode(0o600);
        }
    }
    let file = options
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?;
    check(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn check(file: &File) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = file.metadata()?;
    let mode = metadata.permissions().mode();
    let owner_uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_file()
        && metadata.nlink() == 1
        && metadata.uid() == owner_uid
        && mode & 0o7777 == 0o600
    {
        Ok(())
    } else {
        Err(io::Error::other("invalid private trace file"))
    }
}

#[cfg(windows)]
mod windows {
    use super::{Access, File, Utf8Path, io};
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, RawHandle};
    use std::ptr::null_mut;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW,
        ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo, SDDL_REVISION_1,
        SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetTokenInformation, OWNER_SECURITY_INFORMATION,
        SECURITY_ATTRIBUTES, SetSecurityDescriptorOwner, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_APPEND_DATA, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        GetFileInformationByHandle, OPEN_ALWAYS, OPEN_EXISTING, READ_CONTROL,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    // Disable inherited ACEs and grant full access only through the explicit owner-rights SID.
    const OWNER_ONLY_DACL: &str = "D:P(A;;FA;;;OW)";

    struct LocalAllocation(*mut core::ffi::c_void);
    impl Drop for LocalAllocation {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0) };
            }
        }
    }

    struct Token(HANDLE);
    impl Drop for Token {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { CloseHandle(self.0) };
            }
        }
    }

    struct CurrentUser(Vec<usize>);
    impl CurrentUser {
        fn load() -> io::Result<Self> {
            let mut token = null_mut();
            if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let token = Token(token);
            let mut bytes = 0;
            unsafe {
                GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut bytes);
            }
            if bytes == 0 {
                return Err(io::Error::last_os_error());
            }
            let words = (bytes as usize).div_ceil(size_of::<usize>());
            let mut storage = vec![0usize; words];
            if unsafe {
                GetTokenInformation(
                    token.0,
                    TokenUser,
                    storage.as_mut_ptr().cast(),
                    bytes,
                    &mut bytes,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(Self(storage))
        }

        fn sid(&self) -> windows_sys::Win32::Security::PSID {
            unsafe { (*self.0.as_ptr().cast::<TOKEN_USER>()).User.Sid }
        }
    }

    pub(super) fn open(path: &Utf8Path, access: Access) -> io::Result<File> {
        let user = CurrentUser::load()?;
        let sddl: Vec<u16> = OWNER_ONLY_DACL.encode_utf16().chain(Some(0)).collect();
        let mut descriptor = null_mut();
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let descriptor = LocalAllocation(descriptor);
        if unsafe { SetSecurityDescriptorOwner(descriptor.0, user.sid(), 0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        let wide_path: Vec<u16> = path
            .as_std_path()
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        let (desired_access, disposition) = match access {
            Access::Read => (
                FILE_READ_DATA | FILE_READ_ATTRIBUTES | READ_CONTROL,
                OPEN_EXISTING,
            ),
            Access::Append => (
                FILE_APPEND_DATA | FILE_READ_ATTRIBUTES | READ_CONTROL,
                OPEN_ALWAYS,
            ),
        };
        let handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                desired_access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                &attributes,
                disposition,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle as RawHandle) };
        check(&file, &user)?;
        Ok(file)
    }

    fn check(file: &File, user: &CurrentUser) -> io::Result<()> {
        let handle = file.as_raw_handle() as HANDLE;
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        if unsafe { GetFileInformationByHandle(handle, &mut information) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if information.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)
            != 0
            || information.nNumberOfLinks != 1
        {
            return Err(io::Error::other("invalid private trace file"));
        }

        let mut owner = null_mut();
        let mut descriptor = null_mut();
        let status = unsafe {
            GetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                null_mut(),
                null_mut(),
                null_mut(),
                &mut descriptor,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let descriptor = LocalAllocation(descriptor);
        if owner.is_null()
            || unsafe { windows_sys::Win32::Security::EqualSid(owner, user.sid()) } == 0
        {
            return Err(io::Error::other("private trace file has a different owner"));
        }

        let mut actual_dacl = null_mut();
        let mut dacl_len = 0;
        if unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor.0,
                SDDL_REVISION_1,
                DACL_SECURITY_INFORMATION,
                &mut actual_dacl,
                &mut dacl_len,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let actual_dacl = LocalAllocation(actual_dacl.cast());
        if actual_dacl.0.is_null() || dacl_len == 0 || dacl_len > 128 {
            return Err(io::Error::other("private trace DACL is invalid"));
        }
        let actual =
            unsafe { std::slice::from_raw_parts(actual_dacl.0.cast::<u16>(), dacl_len as usize) };
        let expected: Vec<u16> = OWNER_ONLY_DACL.encode_utf16().collect();
        if actual.len() == expected.len() + 1
            && actual[..expected.len()] == expected
            && actual[expected.len()] == 0
        {
            Ok(())
        } else {
            Err(io::Error::other("private trace DACL is not owner-only"))
        }
    }
}

#[cfg(windows)]
pub(super) use windows::open;

#[cfg(not(any(unix, windows)))]
pub(super) fn open(_path: &Utf8Path, _access: Access) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private trace permissions are unsupported on this platform",
    ))
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn private_file_is_owner_only_and_permissive_existing_files_are_rejected() {
        let dir = std::env::temp_dir().join(format!("qcg-trace-private-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("trace.jsonl")).unwrap();
        let file = open(&path, Access::Append).unwrap();
        assert_eq!(
            file.metadata().unwrap().permissions().mode() & 0o7777,
            0o600
        );
        drop(file);

        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions).unwrap();
        assert!(open(&path, Access::Read).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
#[cfg(windows)]
mod windows_tests {
    use super::*;
    use camino::Utf8PathBuf;
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{FromRawHandle as _, RawHandle};
    use std::ptr::null_mut;
    use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateFileW, FILE_APPEND_DATA, FILE_ATTRIBUTE_NORMAL,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    #[test]
    fn refuses_existing_everyone_readable_trace_file() {
        let dir = std::env::temp_dir().join(format!("qcg-trace-public-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("trace.jsonl")).unwrap();
        let mut sddl: Vec<u16> = "D:P(A;;FA;;;WD)".encode_utf16().chain(Some(0)).collect();
        let mut descriptor = null_mut();
        assert_ne!(
            unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    sddl.as_mut_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    null_mut(),
                )
            },
            0
        );
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        let wide_path: Vec<u16> = path
            .as_std_path()
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        let handle = unsafe {
            CreateFileW(
                wide_path.as_ptr(),
                FILE_APPEND_DATA,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                &attributes,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                null_mut(),
            )
        };
        assert_ne!(handle, INVALID_HANDLE_VALUE);
        drop(unsafe { File::from_raw_handle(handle as RawHandle) });
        unsafe { LocalFree(descriptor) };

        assert!(open(&path, Access::Append).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
