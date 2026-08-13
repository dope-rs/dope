use std::{
    io, mem,
    os::{fd, fd::AsRawFd as _},
    ptr, slice, time,
};

use crate::{
    backend::kqueue::{engine::event, ops},
    platform,
};

pub(crate) struct Poll {
    kq: fd::OwnedFd,
    pub(in crate::backend::kqueue) changes: event::Changes,
    failure: Option<PollFailure>,
    reactor_cursor: u8,
}

#[derive(Clone, Copy)]
struct PollFailure {
    raw_os_error: Option<i32>,
    kind: io::ErrorKind,
}

impl PollFailure {
    fn capture(error: &io::Error) -> Self {
        Self {
            raw_os_error: error.raw_os_error(),
            kind: error.kind(),
        }
    }

    fn into_error(self) -> io::Error {
        match self.raw_os_error {
            Some(raw) => io::Error::from_raw_os_error(raw),
            None => io::Error::from(self.kind),
        }
    }
}

impl Poll {
    fn abi_count(len: usize, message: &'static str) -> io::Result<libc::c_int> {
        use libc::c_int;

        c_int::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, message))
    }

    pub(in crate::backend::kqueue) fn new(
        kq: fd::OwnedFd,
        change_capacity: usize,
    ) -> io::Result<Self> {
        Ok(Self {
            kq,
            changes: event::Changes::try_with_capacity(change_capacity)?,
            failure: None,
            reactor_cursor: 0,
        })
    }

    pub(in crate::backend::kqueue) fn next_reactor_cursor(&mut self) -> usize {
        let cursor = self.reactor_cursor as usize;
        self.reactor_cursor = (self.reactor_cursor + 1) % ops::REACTOR_LANES as u8;
        cursor
    }

    pub(in crate::backend::kqueue) fn clear(&mut self) {
        self.changes.clear();
    }

    pub(in crate::backend::kqueue) fn revoke(&mut self) -> io::Result<()> {
        const RECEIPTS: usize = 64;

        self.check()?;
        let empty = libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        };
        let mut receipts = [empty; RECEIPTS];
        while !self.changes.is_empty() {
            let count = self.changes.len().min(receipts.len());
            let split = self.changes.len() - count;
            for change in &mut self.changes.as_mut_slice()[split..] {
                change.flags |= libc::EV_RECEIPT;
            }
            let count = count as libc::c_int;
            let received = unsafe {
                libc::kevent(
                    self.kq.as_raw_fd(),
                    self.changes.as_slice()[split..].as_ptr(),
                    count,
                    receipts.as_mut_ptr(),
                    count,
                    ptr::null(),
                )
            };
            if received < 0 {
                let error = io::Error::last_os_error();
                return Err(self.fail(error));
            }
            if received != count {
                let error = io::Error::other("dope-kqueue: incomplete revocation receipt set");
                return Err(self.fail(error));
            }
            for receipt in &receipts[..count as usize] {
                if receipt.flags & libc::EV_ERROR == 0 {
                    let error = io::Error::other("dope-kqueue: malformed revocation receipt");
                    return Err(self.fail(error));
                }
                let errno = receipt.data as i32;
                if errno != 0 && errno != libc::ENOENT {
                    return Err(self.fail(io::Error::from_raw_os_error(errno)));
                }
            }
            self.changes.commit_tail(count as usize);
        }
        Ok(())
    }

    fn failure(&self) -> Option<io::Error> {
        self.failure.map(PollFailure::into_error)
    }

    pub(in crate::backend::kqueue) fn is_failed(&self) -> bool {
        self.failure.is_some()
    }

    pub(in crate::backend::kqueue) fn check(&self) -> io::Result<()> {
        match self.failure() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn fail(&mut self, error: io::Error) -> io::Error {
        match self.failure {
            Some(failure) => failure.into_error(),
            None => {
                let failure = PollFailure::capture(&error);
                self.failure = Some(failure);
                failure.into_error()
            }
        }
    }

    pub(in crate::backend::kqueue) fn wait<'events>(
        &mut self,
        events: &'events mut [mem::MaybeUninit<libc::kevent>],
        timeout: Option<time::Duration>,
        changes: &mut event::Budget<'_, '_, event::ChangeLane>,
    ) -> io::Result<event::Events<'events>> {
        self.check()?;
        let timeout = timeout.map(platform::Timeout::try_from).transpose()?;
        let change_count = self.changes.len().min(changes.remaining());
        let submitted_changes = change_count != 0;
        let change_count_abi = match Self::abi_count(
            change_count,
            "dope-kqueue: changelist exceeds kevent ABI capacity",
        ) {
            Ok(count) => count,
            Err(error) => return Err(self.fail(error)),
        };
        let event_count = match Self::abi_count(
            events.len(),
            "dope-kqueue: event buffer exceeds kevent ABI capacity",
        ) {
            Ok(count) => count,
            Err(error) => return Err(self.fail(error)),
        };
        changes.spend(change_count);
        let timeout_storage;
        let timeout_ptr: *const libc::timespec = match timeout.as_ref() {
            None => ptr::null(),
            Some(timeout) => {
                timeout_storage = libc::timespec {
                    tv_sec: timeout.seconds(),
                    tv_nsec: timeout.nanoseconds(),
                };
                &timeout_storage
            }
        };
        let count = unsafe {
            libc::kevent(
                self.kq.as_raw_fd(),
                self.changes.tail(change_count).as_ptr(),
                change_count_abi,
                events.as_mut_ptr().cast(),
                event_count,
                timeout_ptr,
            )
        };
        let initialized = if count < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) && !submitted_changes {
                0
            } else {
                return Err(self.fail(error));
            }
        } else {
            self.changes.commit_tail(change_count);
            count as usize
        };
        // SAFETY: successful `kevent` initializes exactly the returned prefix
        // and never returns more events than the supplied buffer can hold.
        let initialized = unsafe { slice::from_raw_parts(events.as_ptr().cast(), initialized) };
        Ok(event::Events::new(initialized))
    }

    pub(in crate::backend::kqueue) fn register_shutdown(
        &self,
        fd: fd::BorrowedFd<'_>,
    ) -> io::Result<()> {
        let event = libc::kevent {
            ident: fd.as_raw_fd() as libc::uintptr_t,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: event::Udata::shutdown().into_kevent(),
        };
        let result = unsafe {
            libc::kevent(
                self.kq.as_raw_fd(),
                &event,
                1,
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
