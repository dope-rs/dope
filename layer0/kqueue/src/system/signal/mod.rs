use std::{marker, mem, os::fd};

pub(crate) use acquire::Acquire;
use dope_sys::thread::signal;
pub(crate) use wait::Wait;

use crate::system::Queue;

mod acquire;
pub(crate) mod raw;
mod wait;

#[repr(transparent)]
pub struct Source<'a> {
    fd: fd::BorrowedFd<'a>,
    owner: marker::PhantomData<&'a raw::Receiver>,
}

impl<'a> Source<'a> {
    fn new(owner: &'a raw::Receiver) -> Self {
        Self {
            fd: fd::AsFd::as_fd(owner),
            owner: marker::PhantomData,
        }
    }
}

impl signal::Signal for Source<'_> {
    type Error = Wait;
    /// A blocking SIGTERM-only queue and one output slot guarantee one completed event.
    fn wait(&self) -> Result<(), Wait> {
        let mut event = [mem::MaybeUninit::<libc::kevent>::uninit()];
        loop {
            match Queue::wait(self.fd, &[], &mut event, 1, None) {
                Ok(_) => return Ok(()),
                Err(fault) if fault.is_intr() => continue,
                Err(fault) => return Err(Wait::Kevent(fault)),
            }
        }
    }
}

impl fd::AsFd for Source<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.fd
    }
}

const _: () = {
    assert!(mem::size_of::<Option<Source<'_>>>() == mem::size_of::<Option<fd::BorrowedFd<'_>>>());
};
