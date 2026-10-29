use dope_sys::{
    io, kernel,
    probe::session,
    ring::{map, register::Occupied as _},
};
use o3::mem::Rejected;

use crate::{
    error,
    reactor::{register, ring, sq},
};

pub struct Closing<T> {
    ring: ring::Closing<T>,
    file: register::raw::Occupied,
}

pub struct Pending<T> {
    ring: <ring::Closing<T> as io::Retry>::Owner,
    file: register::raw::Occupied,
}
impl<T> Closing<T> {
    pub(crate) const fn new(ring: ring::Closing<T>, file: register::raw::Occupied) -> Self {
        Self { ring, file }
    }
}
impl<T> session::Closed for Closing<T> {
    type Target = sq::Fixed;
    fn slot(&self) -> Self::Target {
        self.file.slot()
    }
}
impl<T> io::Retry for Closing<T> {
    type Failure = <ring::Closing<T> as io::Retry>::Failure;
    type Owner = Pending<T>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        let Self { ring, file } = self;
        ring.recover().map(|ring| Pending { ring, file })
    }
}

impl<T> io::Close for Pending<T> {
    type State = Closing<T>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { ring, file } = self;
        match ring.try_close() {
            Ok(()) => Ok(()),
            Err(ring) => Err(Closing { ring, file }),
        }
    }
}
impl<T> map::Unmap for Closing<T> {
    type Error = kernel::Errno<error::Unmap>;

    fn failure(&self) -> map::Storage<Self::Error> {
        map::Storage::Ring(self.ring.unmap())
    }
}
