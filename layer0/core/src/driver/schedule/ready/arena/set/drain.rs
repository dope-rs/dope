use std::{mem, num};

use o3::mem::Lease;
use ready::arena::set::{Set, Side};

use crate::driver::schedule::ready;

/// One active ready batch tied to its set.
pub(in crate::driver::schedule::ready) struct Drain<'a> {
    lease: Lease<&'a Set>,
}

/// A live descriptor or dispatch index admitted into this set's exact extent.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Queued(
    pub(in crate::driver::schedule::ready) ready::DynamicIndex,
);

impl<'a> Drain<'a> {
    pub(in crate::driver::schedule::ready::arena::set) const fn new(
        set: &'a Set,
        side: Side,
    ) -> Self {
        Self {
            lease: Lease::new(set, side),
        }
    }
}

impl Iterator for Drain<'_> {
    type Item = Queued;

    fn next(&mut self) -> Option<Self::Item> {
        self.lease.sink().store.next(*self.lease.item()).map(Queued)
    }
}

const _: () = assert!(mem::size_of::<Drain<'static>>() == 2 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Option<Drain<'static>>>() == 2 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Queued>() == mem::size_of::<num::NonZero<u16>>());
