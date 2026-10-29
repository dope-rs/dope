use std::{
    mem,
    os::fd::{self, OwnedFd},
    ptr, slice,
};

use dope_sys::{Errno, io::reactor, kernel, table::change};
use fd::{AsFd as _, AsRawFd as _};
use o3::base::marker::ThreadBound;

use crate::Kqueue;

pub(crate) mod signal;

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Queue(fd::OwnedFd, ThreadBound);

impl Queue {
    pub(crate) fn new() -> Result<Self, reactor::Create> {
        use dope_sys::io::{
            fd::{self, Fd as _},
            reactor::Create,
        };

        // SAFETY: kqueue takes no pointers and returns a fresh descriptor or -1.
        let raw = unsafe { libc::kqueue() };
        // SAFETY: raw retains that native result; failure reads its unchanged errno.
        let queue = Self(
            unsafe {
                <OwnedFd as fd::raw::Adopt>::try_adopt::<Errno>(raw, || {
                    kernel::raw::Code::last::<Kqueue>().into()
                })
                .map_err(Create::Open)
            }?,
            ThreadBound::NEW,
        );
        Kqueue::set_retry::<{ libc::F_SETFD }, Errno>(queue.0.as_fd(), libc::FD_CLOEXEC)
            .map_err(Create::Cloexec)?;
        Ok(queue)
    }

    /// Zero output capacity proves that a successful submission returns no events.
    pub(crate) fn submit<C: change::raw::Batch<Event = libc::kevent>>(
        &self,
        changes: &C,
    ) -> Result<(), Errno> {
        Self::wait(self.as_fd(), changes, &mut [], 0, None).map(|_| ())
    }

    pub(crate) fn wait<'a, C: change::raw::Batch<Event = libc::kevent>, const N: usize>(
        file: fd::BorrowedFd<'_>,
        changes: &C,
        events: &'a mut [mem::MaybeUninit<libc::kevent>; N],
        limit: usize,
        timeout: Option<&libc::timespec>,
    ) -> Result<&'a [libc::kevent], Errno> {
        const {
            assert!(C::MAX <= libc::c_int::MAX as usize);
            assert!(N <= libc::c_int::MAX as usize);
        }
        let changes = changes.as_slice();
        let slots = limit.min(N) as libc::c_int;
        let raw = unsafe {
            libc::kevent(
                file.as_raw_fd(),
                changes.as_ptr(),
                changes.len() as libc::c_int,
                events.as_mut_ptr().cast(),
                slots,
                timeout.map_or(ptr::null(), ptr::from_ref),
            )
        };
        // SAFETY: kevent completes at most `slots`, the exact output count borrowed
        // for this call, and failure retains its unchanged errno.
        let count = unsafe {
            kernel::raw::Status::new(raw)
                .try_count::<Errno>(|| kernel::raw::Code::last::<Kqueue>().into())
        }?
        .get();
        Ok(unsafe { slice::from_raw_parts(events.as_ptr().cast(), count) })
    }
}

impl fd::AsFd for Queue {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

const _: () = {
    assert!(mem::size_of::<Option<Queue>>() == mem::size_of::<Option<fd::OwnedFd>>());
};
