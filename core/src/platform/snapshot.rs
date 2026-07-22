use std::error;
use std::fmt::{self, Display, Formatter};
use std::io::{Error, ErrorKind};

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
