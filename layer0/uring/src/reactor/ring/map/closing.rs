use o3::{base::marker::ThreadBound, mem::Rejected};
type Invariant<T> = marker::PhantomData<(ThreadBound, fn(T) -> T)>;

use std::marker;

use dope_sys::{
    io, kernel,
    ring::map::{Storage, Unmap},
};

use crate::{
    error,
    reactor::ring::{self, map},
};

pub struct Closing<T> {
    fd: ring::ffi::Fd,
    maps: map::Closing,
    local: Invariant<T>,
}
impl<T> Closing<T> {
    pub(crate) fn close(
        fd: ring::ffi::Fd,
        maps: map::Set,
        local: Invariant<T>,
    ) -> Result<(), Self> {
        match maps.try_close() {
            Ok(()) => Ok(()),
            Err(maps) => Err(Self { fd, maps, local }),
        }
    }
}
impl<T> Closing<T> {
    pub(crate) fn unmap(&self) -> crate::Unmap {
        self.maps.failure()
    }
}
impl<T> Unmap for Closing<T> {
    type Error = kernel::Errno<error::Unmap>;

    fn failure(&self) -> Storage<Self::Error> {
        Storage::Ring(self.maps.failure())
    }
}
pub struct Owner<T> {
    fd: ring::ffi::Fd,
    maps: map::Pending,
    local: Invariant<T>,
}

impl<T> io::Close for Owner<T> {
    type State = Closing<T>;

    fn try_close(self) -> Result<(), Closing<T>> {
        let Self { fd, maps, local } = self;
        match maps.try_close() {
            Ok(()) => Ok(()),
            Err(maps) => Err(Closing { fd, maps, local }),
        }
    }
}

impl<T> io::Retry for Closing<T> {
    type Failure = crate::Unmap;
    type Owner = Owner<T>;

    fn recover(self) -> Rejected<Self::Owner, Self::Failure> {
        let Self { fd, maps, local } = self;
        maps.recover().map(|maps| Owner { fd, maps, local })
    }
}
const _: () = {
    assert!(std::mem::size_of::<Closing<()>>() == 6 * std::mem::size_of::<usize>());
    assert!(std::mem::align_of::<Closing<()>>() == std::mem::align_of::<usize>());
    assert!(std::mem::size_of::<Result<(), Closing<()>>>() == std::mem::size_of::<Closing<()>>());
};
