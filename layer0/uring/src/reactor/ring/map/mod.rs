pub(in crate::reactor::ring) mod cleanup;
pub(in crate::reactor::ring) mod closing;
pub(in crate::reactor::ring) mod setups;

use std::{mem, num, ptr};

use dope_sys::{
    io::{Close, Retry},
    kernel,
    ring::create,
};
use o3::mem::Rejected;

use crate::{Mapping, error, reactor::ring};

type Cause = create::Cause<kernel::Errno<error::Setup>, kernel::Errno<error::Map>>;

#[derive(Debug)]
pub(crate) struct Region {
    address: *mut libc::c_void,
    len: num::NonZero<usize>,
}

#[derive(Debug)]
pub(crate) struct Unmap {
    region: Region,
    code: kernel::Errno<error::Unmap>,
}

#[derive(Debug)]
pub(crate) struct Closing(State<Unmap>);
pub(crate) struct Pending(State<Region>);

pub(crate) enum Open {
    Failure(Mapping),
    Cleanup { failure: Mapping, unmap: Unmap },
}

#[derive(Debug)]
pub(crate) struct Set(ring::ffi::Map, ring::ffi::Map);

#[derive(Debug)]
enum State<C> {
    One(C),
    Two { current: C, next: ring::ffi::Map },
}

impl Region {
    pub(crate) const fn new(address: *mut libc::c_void, len: num::NonZero<usize>) -> Self {
        Self { address, len }
    }

    pub(crate) const fn address(&self) -> *mut libc::c_void {
        self.address
    }

    pub(crate) const fn len(&self) -> num::NonZero<usize> {
        self.len
    }
}

impl Close for Region {
    type State = Unmap;

    fn try_close(self) -> Result<(), Unmap> {
        ring::ffi::Map::release(self)
    }
}

impl Unmap {
    pub(crate) const fn new(
        address: *mut libc::c_void,
        len: num::NonZero<usize>,
        code: kernel::Errno<error::Unmap>,
    ) -> Self {
        Self {
            region: Region::new(address, len),
            code,
        }
    }

    pub(crate) const fn failure(&self) -> crate::Unmap {
        crate::Unmap::new(self.region.address, self.region.len, self.code)
    }
}

impl Retry for Unmap {
    type Failure = crate::Unmap;
    type Owner = Region;

    fn recover(self) -> Rejected<Region, crate::Unmap> {
        let failure = self.failure();
        Rejected::new(self.region, failure)
    }
}

impl Closing {
    pub(crate) fn failure(&self) -> crate::Unmap {
        match &self.0 {
            State::One(current) | State::Two { current, .. } => current.failure(),
        }
    }

    pub(crate) fn one(first: ring::ffi::Map) -> Result<(), Self> {
        Pending(State::One(first.into_region())).try_close()
    }

    pub(crate) fn two(first: ring::ffi::Map, second: ring::ffi::Map) -> Result<(), Self> {
        Pending(State::Two {
            current: first.into_region(),
            next: second,
        })
        .try_close()
    }

    pub(crate) const fn dirty(unmap: Unmap) -> Self {
        Self(State::One(unmap))
    }

    pub(crate) const fn dirty_two(unmap: Unmap, second: ring::ffi::Map) -> Self {
        Self(State::Two {
            current: unmap,
            next: second,
        })
    }
}

impl Retry for Closing {
    type Failure = crate::Unmap;
    type Owner = Pending;

    fn recover(self) -> Rejected<Pending, crate::Unmap> {
        match self.0 {
            State::One(current) => current
                .recover()
                .map(|current| Pending(State::One(current))),
            State::Two { current, next } => current
                .recover()
                .map(|current| Pending(State::Two { current, next })),
        }
    }
}

impl Close for Pending {
    type State = Closing;

    fn try_close(self) -> Result<(), Closing> {
        match self.0 {
            State::One(current) => current.try_close().map_err(Closing::dirty),
            State::Two { current, next } => match current.try_close() {
                Ok(()) => Closing::one(next),
                Err(current) => Err(Closing(State::Two { current, next })),
            },
        }
    }
}

impl Set {
    pub(crate) const fn single(submit: ring::ffi::Map, entries: ring::ffi::Map) -> Self {
        Self(submit, entries)
    }

    pub(crate) const fn rings(&self) -> &ring::ffi::Map {
        &self.0
    }

    pub(crate) const fn entries(&self) -> &ring::ffi::Map {
        &self.1
    }

    pub(crate) fn try_close(self) -> Result<(), Closing> {
        let Self(submit, entries) = self;
        Closing::two(entries, submit)
    }
}

const _: () = {
    assert!(mem::size_of::<Set>() == 2 * mem::size_of::<ring::ffi::Map>());
    assert!(mem::align_of::<Set>() == mem::align_of::<ring::ffi::Map>());
    assert!(mem::size_of::<crate::Unmap>() == 3 * mem::size_of::<usize>());
    assert!(mem::align_of::<Unmap>() == mem::align_of::<ptr::NonNull<libc::c_void>>());
};
