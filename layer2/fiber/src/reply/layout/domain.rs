use std::{marker, mem, num::NonZero};

/// The immutable lane extent of one generative arena owner.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Domain<'arena> {
    lanes: NonZero<u32>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

impl<'arena> Domain<'arena> {
    pub(in crate::reply) const fn new(lanes: NonZero<u32>) -> Self {
        Self {
            lanes,
            owner: marker::PhantomData,
        }
    }

    pub const fn capacity(self) -> NonZero<u32> {
        self.lanes
    }
}

const _: () = assert!(mem::size_of::<Domain<'static>>() == mem::size_of::<NonZero<u32>>());
