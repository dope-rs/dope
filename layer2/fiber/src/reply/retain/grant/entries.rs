//! Registration capacity ownership.

use std::{mem::size_of, num::NonZero};

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

#[derive(Clone, Copy, Eq, PartialEq)]
struct Remaining(NonZero<usize>);

struct Expand;
struct Take;
struct Last;

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

// SAFETY: Remaining stores the exact nonzero entry count.
unsafe impl mem::raw::Amount<1, usize> for Remaining {
    fn get(self) -> [usize; 1] {
        [self.0.get()]
    }
}

// SAFETY: expansion preserves the complete N-command-plus-terminal acquisition.
unsafe impl<const N: usize> mem::raw::ExactRebind<1, usize, Group<N>> for Expand {
    type Output = Remaining;

    fn rebind(self, _: Group<N>) -> Remaining {
        const { assert!(N > 0 && N < usize::MAX) };
        Remaining(NonZero::<usize>::MIN.saturating_add(N))
    }
}

// SAFETY: Group's const domain proves this expansion for every source value.
unsafe impl<const N: usize> mem::raw::Rebind<1, usize, Group<N>> for Expand {}

// SAFETY: split_group binds Take only for the N command positions of an N+1
// acquisition, so one terminal unit always remains after this exact partition.
unsafe impl mem::raw::Exact<1, usize, Remaining> for Take {
    type Input = ();
    type Left = Remaining;
    type Right = Unit;

    fn parts(self, source: Remaining, (): ()) -> (Remaining, Unit) {
        // SAFETY: the bound command position leaves at least the terminal unit.
        let rest = unsafe { NonZero::new_unchecked(source.0.get() - 1) };
        (Remaining(rest), Unit)
    }
}

// SAFETY: split_group binds Last only after all N command units were removed.
unsafe impl mem::raw::ExactRebind<1, usize, Remaining> for Last {
    type Output = Unit;

    fn rebind(self, _: Remaining) -> Self::Output {
        Unit
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

impl<const N: usize> Group<N> {
    pub(in crate::reply) fn split_group<'credits, T>(
        grant: Grant<'credits, Self>,
        values: [T; N],
    ) -> ([(Grant<'credits, Unit>, T); N], Grant<'credits, Unit>) {
        let mut rest = grant.map(Expand);
        let commands = values.map(|value| {
            // SAFETY: array construction visits exactly N distinct positions;
            // the N+1 acquisition therefore retains one terminal unit throughout.
            (unsafe { rest.bind_mut(Take) }.split(()), value)
        });
        // SAFETY: all N command units were transferred from this exact source.
        let terminal = unsafe { rest.bind(Last) }.map();
        (commands, terminal)
    }
}

const _: () = assert!(size_of::<Grant<'_, Unit>>() == size_of::<usize>());
