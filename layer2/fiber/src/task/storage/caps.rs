use core::{marker, num};

use tree::Member as _;

use crate::task::storage::fixed::tree;

/// Proof that `N` is nonzero and fits the compact slot-index representation.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Slots<const N: usize>(marker::PhantomData<fn() -> num::NonZero<u16>>);

/// Proof that `N` is nonzero and fits the driver scheduler-index representation.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Tasks<const N: usize>(marker::PhantomData<fn() -> num::NonZero<u32>>);

/// Proof that `N` is in the deliberately small inline fiber-group tier.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Inline<const N: usize>(marker::PhantomData<fn() -> num::NonZero<u8>>);

/// Zero-sized proof that an `At` projection lies inside its scheduler tree.
pub(crate) struct Path<R, P> {
    root: marker::PhantomData<fn(R) -> R>,
    path: marker::PhantomData<fn(P) -> P>,
}

impl<const N: usize> Slots<N> {
    pub(crate) fn new() -> Self {
        const { assert!(N > 0 && N <= u16::MAX as usize) };
        Self(marker::PhantomData)
    }
}

impl<const N: usize> Tasks<N> {
    pub(crate) const fn new() -> Self {
        const { assert!(N > 0 && N <= u32::MAX as usize) };
        Self(marker::PhantomData)
    }

    pub(crate) const fn nonzero(self) -> num::NonZero<u32> {
        let Self(marker::PhantomData) = self;
        const { num::NonZero::<u32>::MIN.saturating_add((N - 1) as u32) }
    }
}

impl<const N: usize> Inline<N> {
    pub(crate) fn new() -> Self {
        const { assert!(N > 0 && N <= 16) };
        Self(marker::PhantomData)
    }
}

impl<R, P> Path<R, P> {
    pub(crate) fn new<'d>() -> Self
    where
        R: tree::At<'d, P>,
    {
        const { assert!(R::TASKS.contains(R::OFFSET, R::Member::CAPACITY)) };
        Self {
            root: marker::PhantomData,
            path: marker::PhantomData,
        }
    }

    pub(crate) fn local<'d, const N: usize>(self) -> tree::Offset
    where
        R: tree::At<'d, P>,
    {
        let Self { root: _, path: _ } = self;
        const {
            let capacity = tree::Capacity::new(Tasks::<N>::new().nonzero());
            assert!(R::Member::CAPACITY.is_same(capacity));
        };
        R::OFFSET
    }
}

const _: () = {
    assert!(core::mem::size_of::<Slots<1>>() == 0);
    assert!(core::mem::size_of::<Tasks<1>>() == 0);
    assert!(core::mem::size_of::<Inline<1>>() == 0);
};
