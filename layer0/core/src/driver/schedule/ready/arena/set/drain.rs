use std::{mem, num};

use crate::driver::schedule::ready;

/// A live descriptor or dispatch index admitted into this set's exact extent.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Queued(
    pub(in crate::driver::schedule::ready) ready::DynamicIndex,
);

const _: () = assert!(mem::size_of::<Queued>() == mem::size_of::<num::NonZero<u16>>());
