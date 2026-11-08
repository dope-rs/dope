use core::{marker, num};

/// Proof that `N` is nonzero and fits the compact slot-index representation.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Slots<const N: usize>(marker::PhantomData<fn() -> num::NonZero<u16>>);

/// Proof that `N` is nonzero and fits the driver scheduler-index representation.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Tasks<const N: usize>(marker::PhantomData<fn() -> num::NonZero<u32>>);

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

const _: () = {
    assert!(core::mem::size_of::<Slots<1>>() == 0);
    assert!(core::mem::size_of::<Tasks<1>>() == 0);
};
