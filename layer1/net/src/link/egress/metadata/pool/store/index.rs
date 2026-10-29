use std::{marker, num};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::link::egress) struct Index<'d>(
    num::NonZero<u32>,
    marker::PhantomData<fn(&'d ()) -> &'d ()>,
);

impl<'d> Index<'d> {
    pub(super) const fn new(position: num::NonZero<u32>) -> Self {
        Self(position, marker::PhantomData)
    }

    pub(super) fn offset(self) -> usize {
        (self.0.get() - 1) as usize
    }
}

const _: () = {
    assert!(usize::BITS >= u32::BITS);
    assert!(std::mem::size_of::<Index<'static>>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Option<Index<'static>>>() == std::mem::size_of::<u32>());
};
