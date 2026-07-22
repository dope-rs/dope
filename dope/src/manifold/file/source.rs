use std::marker::PhantomData;
use std::os::fd::OwnedFd;
use std::rc::Rc;

#[derive(Debug)]
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

    #[doc(hidden)]
    pub fn lease(&self) -> Rc<OwnedFd> {
        Rc::clone(&self.fd)
    }
}
