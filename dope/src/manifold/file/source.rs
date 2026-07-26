use std::marker::PhantomData;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::rc::Rc;

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Source<'d> {
    fd: Rc<OwnedFd>,
    brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Source<'d> {
    #[doc(hidden)]
    pub fn owned(fd: OwnedFd) -> Self {
        Self {
            fd: Rc::new(fd),
            brand: PhantomData,
        }
    }
}

impl AsRawFd for Source<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
