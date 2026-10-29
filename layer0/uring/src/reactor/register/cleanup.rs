use dope_sys::{
    io::{Close as _, Retry as _},
    kernel, memory,
    ring::map,
};

use crate::{error, reactor::ring::Closing};

pub struct Cleanup<T> {
    ring: Closing<T>,
    error: kernel::Errno<error::Register<20>>,
}
impl<T> Cleanup<T> {
    pub(crate) const fn new(ring: Closing<T>, error: kernel::Errno<error::Register<20>>) -> Self {
        Self { ring, error }
    }
}
impl<T> memory::Cleanup<kernel::Errno<error::Register<20>>> for Cleanup<T> {
    type Error = map::Failure<kernel::Errno<error::Unmap>>;
    fn primary(&self) -> kernel::Errno<error::Register<20>> {
        self.error
    }

    fn failure(&self) -> map::Failure<kernel::Errno<error::Unmap>> {
        self.ring.unmap()
    }

    fn try_close(
        self,
        mut report: impl FnMut(Self::Error),
    ) -> Result<kernel::Errno<error::Register<20>>, Self> {
        let Self { ring, error } = self;
        let (ring, cause) = ring.recover().into_parts();
        report(cause);
        match ring.try_close() {
            Ok(()) => Ok(error),
            Err(ring) => Err(Self { ring, error }),
        }
    }
}
