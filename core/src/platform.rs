use std::error;
use std::fmt::{self, Display, Formatter};
use std::io::{Error, ErrorKind};

use crate::backend;
use crate::io::file::RawMetadata;

pub trait Platform {
    type Sqe;
    type Gso;
    type StatBuf;
    type TimerSpec;
    fn entropy() -> std::io::Result<[u64; 2]>;
    fn parse_meta(raw: &Self::StatBuf) -> RawMetadata;
    fn snapshot() -> std::io::Result<Snapshot>;
}

cfg_select! {
    target_os = "linux" => {
        use std::fs;
        use std::mem::{MaybeUninit, size_of};

        use crate::driver::Driver;

        impl Platform for Driver {
            type Sqe = backend::uring::sqe::Sqe;
            type Gso = backend::uring::platform::gso::Gso;
            type StatBuf = libc::statx;
            type TimerSpec = io_uring::types::Timespec;

            fn entropy() -> std::io::Result<[u64; 2]> {
                let mut words = MaybeUninit::<[u64; 2]>::uninit();
                let mut data = words.as_mut_ptr().cast::<u8>();
                let mut len = size_of::<[u64; 2]>();
                while len != 0 {
                    // SAFETY: data/len stay inside the uninitialized words buffer.
                    let written = unsafe { libc::getrandom(data.cast(), len, 0) };
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
                    // SAFETY: written <= len keeps the cursor inside the buffer.
                    data = unsafe { data.add(written) };
                    len -= written;
                }
                // SAFETY: the loop wrote every byte of words.
                Ok(unsafe { words.assume_init() })
            }

            fn parse_meta(raw: &Self::StatBuf) -> RawMetadata {
                RawMetadata {
                    len: raw.stx_size,
                    modified: Some((raw.stx_mtime.tv_sec, raw.stx_mtime.tv_nsec)),
                    regular: u32::from(raw.stx_mode) & libc::S_IFMT == libc::S_IFREG,
                }
            }

            fn snapshot() -> std::io::Result<Snapshot> {
                Ok(Snapshot {
                    syncookies: read_u32("/proc/sys/net/ipv4/tcp_syncookies")? != 0,
                    max_syn_backlog: read_u32("/proc/sys/net/ipv4/tcp_max_syn_backlog")?,
                    somaxconn: read_u32("/proc/sys/net/core/somaxconn")?,
                    rlimit_nofile: OpenFileLimit::get()?.soft(),
                })
            }
        }

        fn read_u32(path: &str) -> std::io::Result<u32> {
            let value = fs::read_to_string(path)?;
            value
                .trim()
                .parse::<u32>()
                .map_err(|error| Error::new(ErrorKind::InvalidData, format!("{path}: {error}")))
        }
    }
    _ => {
        use std::ffi::CStr;
        use std::mem::{MaybeUninit, size_of};
        use std::ptr::null_mut;

        use crate::driver::Driver;

        impl Platform for Driver {
            type Sqe = backend::kqueue::sqe::Sqe;
            type Gso = backend::kqueue::platform::gso::Gso;
            type StatBuf = libc::stat;
            type TimerSpec = backend::kqueue::sqe::TimerSpec;

            fn entropy() -> std::io::Result<[u64; 2]> {
                let mut words = MaybeUninit::<[u64; 2]>::uninit();
                loop {
                    // SAFETY: the pointer/length name the whole words buffer.
                    if unsafe { libc::getentropy(words.as_mut_ptr().cast(), size_of::<[u64; 2]>()) } == 0 {
                        // SAFETY: getentropy filled the buffer.
                        return Ok(unsafe { words.assume_init() });
                    }
                    let error = Error::last_os_error();
                    if error.kind() != ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
            }

            fn parse_meta(raw: &Self::StatBuf) -> RawMetadata {
                RawMetadata {
                    len: u64::try_from(raw.st_size).unwrap_or(0),
                    modified: u32::try_from(raw.st_mtime_nsec)
                        .ok()
                        .filter(|nanos| *nanos < 1_000_000_000)
                        .map(|nanos| (raw.st_mtime, nanos)),
                    regular: (raw.st_mode as libc::mode_t) & libc::S_IFMT == libc::S_IFREG,
                }
            }

            fn snapshot() -> std::io::Result<Snapshot> {
                let somaxconn = sysctl_u32(c"kern.ipc.somaxconn")?;
                Ok(Snapshot {
                    syncookies: true,
                    max_syn_backlog: somaxconn,
                    somaxconn,
                    rlimit_nofile: OpenFileLimit::get()?.soft(),
                })
            }
        }

        fn sysctl_u32(name: &CStr) -> std::io::Result<u32> {
            let mut value: libc::c_int = 0;
            let mut len = size_of::<libc::c_int>();
            // SAFETY: value/len are live locals sized for the sysctl result.
            let rc = unsafe {
                libc::sysctlbyname(
                    name.as_ptr(),
                    (&mut value as *mut libc::c_int).cast(),
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
}

const SYN_BACKLOG_FLOOR: u32 = 4096;
const SOMAXCONN_FLOOR: u32 = 4096;

#[derive(Debug)]
pub struct Snapshot {
    pub syncookies: bool,
    pub max_syn_backlog: u32,
    pub somaxconn: u32,
    pub rlimit_nofile: u64,
}

impl Snapshot {
    pub fn check_slots(&self, requested_slots: u32) -> Result<(), Mismatch> {
        if u64::from(requested_slots) > self.rlimit_nofile {
            return Err(Mismatch::NoFileTooLow {
                requested: requested_slots,
                rlimit: self.rlimit_nofile,
            });
        }
        if !self.syncookies && self.max_syn_backlog < SYN_BACKLOG_FLOOR {
            return Err(Mismatch::SynFloodVulnerable {
                backlog: self.max_syn_backlog,
                expected: SYN_BACKLOG_FLOOR,
            });
        }
        if self.somaxconn < SOMAXCONN_FLOOR {
            return Err(Mismatch::SomaxconnTooLow {
                requested: SOMAXCONN_FLOOR,
                kernel: self.somaxconn,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum Mismatch {
    NoFileTooLow { requested: u32, rlimit: u64 },
    SomaxconnTooLow { requested: u32, kernel: u32 },
    SynFloodVulnerable { backlog: u32, expected: u32 },
}

impl Display for Mismatch {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoFileTooLow { requested, rlimit } => write!(
                formatter,
                "RLIMIT_NOFILE ({rlimit}) below configured fixed_file_slots ({requested}); raise the limit or lower the slot count"
            ),
            Self::SomaxconnTooLow { requested, kernel } => write!(
                formatter,
                "somaxconn ({kernel}) below required floor ({requested}); raise it to {requested}"
            ),
            Self::SynFloodVulnerable { backlog, expected } => write!(
                formatter,
                "SYN-flood vulnerable: syncookies off and max_syn_backlog ({backlog}) below floor ({expected})"
            ),
        }
    }
}

impl error::Error for Mismatch {}

impl From<Mismatch> for Error {
    fn from(mismatch: Mismatch) -> Self {
        Error::new(ErrorKind::InvalidInput, mismatch)
    }
}

pub(crate) struct OpenFileLimit(libc::rlimit);

impl OpenFileLimit {
    pub(crate) fn get() -> std::io::Result<Self> {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
        if rc != 0 {
            return Err(Error::last_os_error());
        }
        Ok(Self(limit))
    }

    #[allow(clippy::unnecessary_cast)]
    pub(crate) fn soft(&self) -> u64 {
        self.0.rlim_cur as u64
    }

    pub(crate) fn raise(mut self) -> std::io::Result<()> {
        self.0.rlim_cur = self.0.rlim_max;
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) };
        if rc != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}
