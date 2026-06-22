use std::fs;
use std::io;

#[derive(Debug)]
pub struct Snapshot {
    pub syncookies: bool,
    pub max_syn_backlog: u32,
    pub somaxconn: u32,
    pub rlimit_nofile: u64,
}

impl Snapshot {
    pub fn detect() -> io::Result<Self> {
        Ok(Self {
            syncookies: Self::read_u32("/proc/sys/net/ipv4/tcp_syncookies")? != 0,
            max_syn_backlog: Self::read_u32("/proc/sys/net/ipv4/tcp_max_syn_backlog")?,
            somaxconn: Self::read_u32("/proc/sys/net/core/somaxconn")?,
            rlimit_nofile: Self::read_rlimit(libc::RLIMIT_NOFILE)?,
        })
    }

    pub fn raise_nofile() -> io::Result<()> {
        let mut rlim = Self::rlimit(libc::RLIMIT_NOFILE)?;
        rlim.rlim_cur = rlim.rlim_max;
        // SAFETY: `rlim` is a valid rlimit with cur set to hard max.
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn rlimit(resource: libc::__rlimit_resource_t) -> io::Result<libc::rlimit> {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `rlim` is a valid, writable rlimit for the duration of the call.
        let rc = unsafe { libc::getrlimit(resource, &mut rlim) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(rlim)
    }

    fn read_u32(path: &str) -> io::Result<u32> {
        let s = fs::read_to_string(path)?;
        s.trim()
            .parse::<u32>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{path}: {e}")))
    }

    fn read_rlimit(resource: libc::__rlimit_resource_t) -> io::Result<u64> {
        Ok(Self::rlimit(resource)?.rlim_max)
    }
}

impl Snapshot {
    pub fn check(&self, cfg: &super::Config) -> Result<(), Mismatch> {
        let requested_slots = cfg.fixed_file_slots;
        if (requested_slots as u64) > self.rlimit_nofile {
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

const SYN_BACKLOG_FLOOR: u32 = 4096;
const SOMAXCONN_FLOOR: u32 = 4096;

#[derive(Debug)]
pub enum Mismatch {
    NoFileTooLow { requested: u32, rlimit: u64 },
    SomaxconnTooLow { requested: u32, kernel: u32 },
    SynFloodVulnerable { backlog: u32, expected: u32 },
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoFileTooLow { requested, rlimit } => write!(
                f,
                "RLIMIT_NOFILE hard cap ({rlimit}) below configured fixed_file_slots ({requested}); raise the rlimit or lower the slot count"
            ),
            Self::SomaxconnTooLow { requested, kernel } => write!(
                f,
                "net.core.somaxconn ({kernel}) below required floor ({requested}); raise via sysctl -w net.core.somaxconn={requested}"
            ),
            Self::SynFloodVulnerable { backlog, expected } => write!(
                f,
                "SYN-flood vulnerable: tcp_syncookies=0 and tcp_max_syn_backlog ({backlog}) below floor ({expected}); enable syncookies or raise the backlog"
            ),
        }
    }
}

impl std::error::Error for Mismatch {}

impl From<Mismatch> for io::Error {
    fn from(m: Mismatch) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, m.to_string())
    }
}
