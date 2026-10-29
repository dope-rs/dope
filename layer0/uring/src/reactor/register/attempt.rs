use dope_sys::{io::Close as _, kernel, ring::register};

use crate::{Ring, error, reactor::register::Cleanup};

#[derive(Debug)]
pub struct Attempt<T> {
    ring: Ring<T>,
    error: kernel::Errno<error::Register<20>>,
}
impl<T> Attempt<T> {
    pub(crate) const fn new(ring: Ring<T>, error: kernel::Errno<error::Register<20>>) -> Self {
        Self { ring, error }
    }
}
impl<T> register::Registration for Attempt<T> {
    type Error = kernel::Errno<error::Register<20>>;
    type Unmap = kernel::Errno<error::Unmap>;
    type Cleanup = Cleanup<T>;

    fn try_close(self) -> Result<Self::Error, Self::Cleanup> {
        let Self { ring, error } = self;
        match ring.try_close() {
            Ok(()) => Ok(error),
            Err(ring) => Err(Cleanup::new(ring, error)),
        }
    }
}
