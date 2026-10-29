use std::{
    marker,
    os::fd::{AsRawFd as _, BorrowedFd},
    slice,
};

pub use libc::nfds_t;

use o3::base::marker::ThreadBound;

use crate::{
    Errno,
    io::poll,
    kernel::{self, errno},
};

pub mod raw;

/// Largest registration count the native poll call accepts.
pub const MAX_ENTRIES: usize = nfds_t::MAX as usize;

/// One native poll registration and the events the kernel reported for it.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Entry<'fd>(
    libc::pollfd,
    marker::PhantomData<(BorrowedFd<'fd>, ThreadBound)>,
);

impl<'fd> Entry<'fd> {
    /// Registers one descriptor for readability.
    pub fn readable(fd: BorrowedFd<'fd>) -> Self {
        Self::new(fd, libc::POLLIN)
    }

    /// Registers one descriptor for writability.
    pub fn writable(fd: BorrowedFd<'fd>) -> Self {
        Self::new(fd, libc::POLLOUT)
    }

    fn new(fd: BorrowedFd<'fd>, events: libc::c_short) -> Self {
        Self(
            libc::pollfd {
                fd: fd.as_raw_fd(),
                events,
                revents: 0,
            },
            marker::PhantomData,
        )
    }

    /// Whether the kernel reported any event for this registration.
    pub const fn is_ready(self) -> bool {
        self.0.revents != 0
    }

    /// Whether the kernel reported readability.
    pub const fn is_readable(self) -> bool {
        self.0.revents & libc::POLLIN != 0
    }

    /// Raw event bits reported for this registration.
    pub const fn revents(self) -> i16 {
        self.0.revents
    }

    /// Stops polling this registration after its owner handles the terminal event.
    pub fn retire(&mut self) {
        self.0.fd = -1;
        self.0.revents = 0;
    }
}

/// Projects the raw registrations for the native wait call.
fn raw<'a>(entries: &'a mut [Entry<'_>]) -> &'a mut [libc::pollfd] {
    // SAFETY: Entry is repr(transparent) over pollfd, so this slice has the exact
    // layout and length of the entries it borrows.
    unsafe { slice::from_raw_parts_mut(entries.as_mut_ptr().cast(), entries.len()) }
}

pub struct Events<'a, 'fd> {
    entries: &'a [Entry<'fd>],
    count: usize,
}

impl<'a, 'fd> Events<'a, 'fd> {
    pub const fn count(&self) -> usize {
        self.count
    }
    pub const fn entries(&self) -> &'a [Entry<'fd>] {
        self.entries
    }
}

/// Native polling against a caller-owned, bounded descriptor collection.
pub trait Poll: errno::Source {
    fn wait<'a, 'fd: 'a, T: poll::raw::Entries<'fd>, E: From<Errno>>(
        owner: &'a mut T,
        timeout: i32,
    ) -> Result<poll::Events<'a, 'fd>, E> {
        const {
            assert!(T::MAX as nfds_t as usize == T::MAX);
        }
        let entries = owner.entries();
        let raw = raw(entries);
        // SAFETY: the entries stay live and uniquely borrowed for the call.
        let raw = unsafe { libc::poll(raw.as_mut_ptr(), raw.len() as nfds_t, timeout) };
        // SAFETY: poll returns -1 or the number of ready registrations it was given.
        let count = unsafe {
            kernel::raw::Status::new(raw)
                .try_count::<E>(|| kernel::raw::Code::last::<Self>().into())
        }?
        .get();
        Ok(poll::Events { entries, count })
    }
}

impl<T: errno::Source> Poll for T {}
