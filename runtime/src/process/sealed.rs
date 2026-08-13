use std::{io, mem, process, ptr};

pub(crate) struct Limit(libc::rlimit);

pub(crate) struct Set(libc::sigset_t);

pub(crate) struct Blocked {
    signals: Set,
    previous: Set,
    _thread: o3::ThreadBound,
}

impl Limit {
    pub(crate) fn current() -> io::Result<Self> {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` points to writable storage for one resource limit.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(limit))
    }

    pub(crate) fn raise(mut self) -> io::Result<()> {
        self.0.rlim_cur = self.0.rlim_max;
        // SAFETY: this limit was initialized by `getrlimit`; raising the soft limit
        // to the existing hard limit preserves that hard limit.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Set {
    pub(crate) fn termination() -> io::Result<Self> {
        let mut set = mem::MaybeUninit::<libc::sigset_t>::uninit();
        // SAFETY: `set` names writable storage for one signal set.
        if unsafe { libc::sigemptyset(set.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful `sigemptyset` initialized `set`; SIGTERM is valid
        // on every supported target.
        if unsafe { libc::sigaddset(set.as_mut_ptr(), libc::SIGTERM) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both initialization calls succeeded.
        Ok(Self(unsafe { set.assume_init() }))
    }

    pub(crate) fn block(self) -> io::Result<super::Blocked> {
        use o3::ThreadBound;

        let mut previous = mem::MaybeUninit::<libc::sigset_t>::uninit();
        // SAFETY: both pointers name valid storage for the complete call.
        let result =
            unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &self.0, previous.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result));
        }
        // SAFETY: successful `pthread_sigmask` initialized `previous`.
        Ok(Blocked {
            signals: self,
            previous: Self(unsafe { previous.assume_init() }),
            _thread: ThreadBound::NEW,
        })
    }

    fn restore(&self) -> io::Result<()> {
        // SAFETY: this is the initialized mask returned by `pthread_sigmask`
        // on this exact execution context; no output mask is requested.
        let result = unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &self.0, ptr::null_mut()) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(result))
        }
    }
}

impl Blocked {
    pub(crate) fn wait(&self) -> io::Result<()> {
        let mut signal = 0;
        // SAFETY: the set is initialized and blocked by this owning value;
        // `signal` is writable for one signal number.
        let result = unsafe { libc::sigwait(&self.signals.0, &mut signal) };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result));
        }
        if signal != libc::SIGTERM {
            return Err(io::Error::other(
                "signal set returned a non-termination signal",
            ));
        }
        Ok(())
    }
}

impl Drop for Blocked {
    fn drop(&mut self) {
        if self.previous.restore().is_err() {
            process::abort();
        }
    }
}
