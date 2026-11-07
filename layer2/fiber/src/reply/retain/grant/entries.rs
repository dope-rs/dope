//! Registration capacity ownership.

use std::mem::size_of;

use o3::mem;

use crate::reply::retain;

pub(in crate::reply) enum Tag {}

pub(in crate::reply) type Grant<'credits, A> =
    mem::raw::Grant<retain::raw::Lane<'credits, 1, Tag>, 1, A>;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Unit;

/// A nonempty command array followed by its distinct terminal entry.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Group<const N: usize>;

impl<const N: usize> Group<N> {
    pub(in crate::reply) const VALUE: Self = {
        assert!(N > 0 && N < usize::MAX);
        Self
    };
}

#[derive(Clone, Copy)]
pub(in crate::reply) struct Entries(u32);

// SAFETY: Unit represents exactly one entry.
unsafe impl mem::raw::Amount<1, usize> for Unit {
    fn get(self) -> [usize; 1] {
        [1]
    }
}

// SAFETY: this amount includes exactly N command entries and one terminal entry.
unsafe impl<const N: usize> mem::raw::Amount<1, usize> for Group<N> {
    fn get(self) -> [usize; 1] {
        const { assert!(N > 0 && N < usize::MAX) };
        [N + 1]
    }
}

impl Entries {
    pub(in crate::reply) const EMPTY: Self = Self(0);

    /// Admits the entry units this exact amount carries.
    pub(in crate::reply) fn admit(self, amount: impl mem::raw::Amount<1, usize>) -> Self {
        Self(self.0 + amount.get()[0] as u32)
    }

    /// Releases the entry units this exact amount carries.
    pub(in crate::reply) fn release(self, amount: impl mem::raw::Amount<1, usize>) -> Self {
        Self(self.0 - amount.get()[0] as u32)
    }

    pub(in crate::reply) const fn get(self) -> usize {
        self.0 as usize
    }
}

const _: () = assert!(size_of::<Grant<'_, Unit>>() == size_of::<usize>());
