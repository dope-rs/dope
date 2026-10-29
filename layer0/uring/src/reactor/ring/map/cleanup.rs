use dope_sys::{
    io::{Close as _, Retry as _},
    memory,
};

use crate::{
    Unmap,
    reactor::ring::{self, map, map::Cause},
};

#[derive(Debug)]
pub struct Cleanup {
    fd: Option<ring::ffi::Fd>,
    failure: Cause,
    maps: map::Closing,
}
impl Cleanup {
    pub(crate) const fn new(fd: Option<ring::ffi::Fd>, failure: Cause, maps: map::Closing) -> Self {
        Self { fd, failure, maps }
    }
}
impl memory::Cleanup<Cause> for Cleanup {
    type Error = Unmap;

    fn primary(&self) -> Cause {
        self.failure
    }

    fn failure(&self) -> Unmap {
        self.maps.failure()
    }

    fn try_close(self, mut report: impl FnMut(Unmap)) -> Result<Cause, Self> {
        let Self { fd, failure, maps } = self;
        let (maps, cause) = maps.recover().into_parts();
        report(cause);
        match maps.try_close() {
            Ok(()) => Ok(failure),
            Err(maps) => Err(Self { fd, failure, maps }),
        }
    }
}
