//! Shared Linux pre-exec membership mechanism; policy supplies the exact run boundary.
use crate::HostCgroupScope;

/// Place a child before payload execution, with fork-to-placement owner-death protection.
#[cfg(target_os = "linux")]
pub fn apply_scope(
    command: &mut std::process::Command,
    scope: &HostCgroupScope,
) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::process::CommandExt as _;
    if crate::validate_parent_path(&scope.delegated_parent).is_err()
        || scope.run_root
            != scope
                .delegated_parent
                .join(crate::run_root_name(&scope.run_id, &scope.nonce))
        || scope.nonce.len() != 32
        || !scope.nonce.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(std::io::Error::other(
            "refusing to launch outside the delegated parent",
        ));
    }
    let procs = scope.run_root.join("cgroup.procs");
    let bytes = procs.as_os_str().as_bytes().to_vec();
    let c_path = std::ffi::CString::new(bytes)
        .map_err(|_| std::io::Error::other("run root path is not C-safe"))?;
    let owner_pid = unsafe { libc::getpid() };
    // SAFETY: `pre_exec` runs after fork and before exec in the child only.
    // The closure performs only async-signal-safe raw syscalls (`open`,
    // `write`, `close`) on a pre-built NUL path and a stack pid buffer, with
    // no allocation, locking, or Rust I/O.
    unsafe {
        command.pre_exec(move || {
            // Cover owner death before membership, including before this prctl.
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != owner_pid {
                return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
            }
            let pid = libc::getpid();
            // SAFETY: `c_path` is a valid NUL-terminated path owned by the closure.
            let fd = libc::open(
                c_path.as_ptr(),
                libc::O_WRONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            );
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut buffer = [0_u8; 16];
            let mut value = pid as u32;
            let length: usize = if value == 0 {
                buffer[0] = b'0';
                1
            } else {
                let mut digits = [0_u8; 16];
                let mut count = 0_usize;
                while value > 0 {
                    digits[count] = b'0' + (value % 10) as u8;
                    value /= 10;
                    count += 1;
                }
                let mut index = 0_usize;
                while index < count {
                    buffer[index] = digits[count - 1 - index];
                    index += 1;
                }
                count
            };
            // SAFETY: `buffer[..length]` is valid stack memory; `fd` is open.
            let mut written = 0_usize;
            while written < length {
                let result = libc::write(
                    fd,
                    buffer[written..length].as_ptr().cast::<libc::c_void>(),
                    length - written,
                );
                if result <= 0 {
                    let error = std::io::Error::last_os_error();
                    libc::close(fd);
                    return Err(error);
                }
                written += result as usize;
            }
            libc::close(fd);
            // Cgroup now owns cleanup; don't depend on a Tokio thread lifetime.
            if libc::prctl(libc::PR_SET_PDEATHSIG, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}
