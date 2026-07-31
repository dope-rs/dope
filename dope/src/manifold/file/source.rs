use std::io;
use std::marker::PhantomData;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

#[derive(Debug)]
#[repr(transparent)]
pub struct Source<'d> {
    fd: OwnedFd,
    brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Source<'d> {
    #[doc(hidden)]
    pub fn owned(fd: OwnedFd) -> Self {
        Self {
            fd,
            brand: PhantomData,
        }
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        self.fd.try_clone().map(Self::owned)
    }
}

impl AsRawFd for Source<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
