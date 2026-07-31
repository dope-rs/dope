use libc::{MCL_CURRENT, MCL_FUTURE, RLIMIT_NOFILE, getrlimit, mlockall, rlimit, setrlimit};
use std::io::{self, Error};

pub(crate) fn lock_memory_best_effort() {
    // SAFETY: `mlockall` takes flags only and does not dereference caller memory.
    // Locking is an optimization: lacking the required limit or capability must
    // not prevent the runtime from starting.
    let _ = unsafe { mlockall(MCL_CURRENT | MCL_FUTURE) };
}

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
