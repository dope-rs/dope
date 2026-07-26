pub(crate) static HOST: Host = Host;

pub(crate) struct Host;

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::read_to_string;
    use std::io::{self, Error, ErrorKind};
    use std::mem::{MaybeUninit, size_of};

    use libc::{S_IFMT, S_IFREG, getrandom, statx};

    use crate::io::file::RawMetadata;
    use crate::platform::raw::file::FileLimit;
    use crate::platform::snapshot::Snapshot;

    use super::Host;

    impl Host {
        pub(crate) fn entropy(&self) -> io::Result<[u64; 2]> {
            let mut words = MaybeUninit::<[u64; 2]>::uninit();
            let mut data = words.as_mut_ptr().cast::<u8>();
            let mut len = size_of::<[u64; 2]>();
            while len != 0 {
                let written = unsafe { getrandom(data.cast(), len, 0) };
                if written < 0 {
                    let error = Error::last_os_error();
                    if error.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error);
                }
                if written == 0 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "kernel entropy returned no bytes",
                    ));
                }
                let written = written as usize;
                data = unsafe { data.add(written) };
                len -= written;
            }
            Ok(unsafe { words.assume_init() })
        }

        pub(crate) fn parse_meta(&self, raw: &statx) -> io::Result<RawMetadata> {
            if raw.stx_mtime.tv_nsec >= 1_000_000_000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "statx returned an invalid modification nanosecond",
                ));
            }
            Ok(RawMetadata {
                len: raw.stx_size,
                modified: Some((raw.stx_mtime.tv_sec, raw.stx_mtime.tv_nsec)),
                regular: u32::from(raw.stx_mode) & S_IFMT == S_IFREG,
            })
        }

        pub(crate) fn snapshot(&self) -> io::Result<Snapshot> {
            Ok(Snapshot {
                syncookies: read_u32("/proc/sys/net/ipv4/tcp_syncookies")? != 0,
                max_syn_backlog: read_u32("/proc/sys/net/ipv4/tcp_max_syn_backlog")?,
                somaxconn: read_u32("/proc/sys/net/core/somaxconn")?,
                rlimit_nofile: FileLimit::get()?.soft(),
            })
        }
    }

    fn read_u32(path: &str) -> io::Result<u32> {
        let value = read_to_string(path)?;
        value
            .trim()
            .parse::<u32>()
            .map_err(|error| Error::new(ErrorKind::InvalidData, format!("{path}: {error}")))
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::ffi::CStr;
    use std::io::{self, Error, ErrorKind};
    use std::mem::{MaybeUninit, size_of};
    use std::ptr::null_mut;

    use libc::{S_IFMT, S_IFREG, c_int, getentropy, mode_t, stat, sysctlbyname};

    use crate::io::file::RawMetadata;
    use crate::platform::raw::file::FileLimit;
    use crate::platform::snapshot::Snapshot;

    use super::Host;

    impl Host {
        pub(crate) fn entropy(&self) -> io::Result<[u64; 2]> {
            let mut words = MaybeUninit::<[u64; 2]>::uninit();
            loop {
                if unsafe { getentropy(words.as_mut_ptr().cast(), size_of::<[u64; 2]>()) } == 0 {
                    return Ok(unsafe { words.assume_init() });
                }
                let error = Error::last_os_error();
                if error.kind() != ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }

        pub(crate) fn parse_meta(&self, raw: &stat) -> io::Result<RawMetadata> {
            let len = u64::try_from(raw.st_size)
                .map_err(|_| Error::new(ErrorKind::InvalidData, "stat returned a negative size"))?;
            let nanos = u32::try_from(raw.st_mtime_nsec).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    "stat returned a negative modification nanosecond",
                )
            })?;
            if nanos >= 1_000_000_000 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "stat returned an invalid modification nanosecond",
                ));
            }
            Ok(RawMetadata {
                len,
                modified: Some((raw.st_mtime, nanos)),
                regular: (raw.st_mode as mode_t) & S_IFMT == S_IFREG,
            })
        }

        pub(crate) fn snapshot(&self) -> io::Result<Snapshot> {
            let somaxconn = sysctl_u32(c"kern.ipc.somaxconn")?;
            Ok(Snapshot {
                syncookies: true,
                max_syn_backlog: somaxconn,
                somaxconn,
                rlimit_nofile: FileLimit::get()?.soft(),
            })
        }
    }

    fn sysctl_u32(name: &CStr) -> io::Result<u32> {
        let mut value: c_int = 0;
        let mut len = size_of::<c_int>();
        let rc = unsafe {
            sysctlbyname(
                name.as_ptr(),
                (&mut value as *mut c_int).cast(),
                &mut len,
                null_mut(),
                0,
            )
        };
        if rc != 0 {
            return Err(Error::last_os_error());
        }
        u32::try_from(value).map_err(|_| Error::new(ErrorKind::InvalidData, "negative sysctl"))
    }
}
