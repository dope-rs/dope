use libc::RLIMIT_NOFILE;
use libc::getrlimit;
use libc::rlimit;
use libc::setrlimit;
use std::io::{self, Error};

pub(crate) struct FileLimit(rlimit);

impl FileLimit {
    pub(crate) fn get() -> io::Result<Self> {
        let mut limit = rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let rc = unsafe { getrlimit(RLIMIT_NOFILE, &mut limit) };
        if rc != 0 {
            return Err(Error::last_os_error());
        }
        Ok(Self(limit))
    }

    #[allow(clippy::unnecessary_cast)]
    pub(crate) fn soft(&self) -> u64 {
        self.0.rlim_cur as u64
    }

    pub(crate) fn raise(mut self) -> io::Result<()> {
        self.0.rlim_cur = self.0.rlim_max;
        let rc = unsafe { setrlimit(RLIMIT_NOFILE, &self.0) };
        if rc != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}
