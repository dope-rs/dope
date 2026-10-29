//! Registration capacity ownership.

use std::{mem::size_of, num::NonZero};

use o3::mem;

use crate::reply::retain;

pub(in crate::reply) enum Tag {}

type Credit<'credits, A> = mem::raw::Grant<retain::raw::Lane<'credits, 2, Tag>, 2, A>;

pub(in crate::reply) struct Grant<'credits, A: mem::raw::Amount<2, usize>>(Credit<'credits, A>);

/// One exact two-axis entry amount, carried by the type instead of an impl.
///
/// The components are this struct's own parameters, so the entry accounting
/// table and the [`mem::raw::Amount`] vector cannot drift apart.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Units<const ENTRIES: usize, const ORDERS: usize>;

/// A nonempty command array followed by its distinct terminal entry.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Group<const N: usize>;

#[derive(Clone, Copy, Eq, PartialEq)]
struct Remaining(NonZero<usize>);

struct Expand;
struct Take;
struct Last;

impl<const ENTRIES: usize, const ORDERS: usize> Units<ENTRIES, ORDERS> {
    pub(in crate::reply) const VALUE: Self = Self;
}

impl<const N: usize> Group<N> {
    pub(in crate::reply) const VALUE: Self = {
        assert!(N > 0 && N < usize::MAX);
        Self
    };
}

#[derive(Clone, Copy)]
pub(in crate::reply) struct Entries(u32);

// SAFETY: `get` returns exactly this type's two parameters.
unsafe impl<const ENTRIES: usize, const ORDERS: usize> mem::raw::Amount<2, usize>
    for Units<ENTRIES, ORDERS>
{
    fn get(self) -> [usize; 2] {
        [ENTRIES, ORDERS]
    }
}

// SAFETY: this amount includes exactly N command entries and one terminal entry.
unsafe impl<const N: usize> mem::raw::Amount<2, usize> for Group<N> {
    fn get(self) -> [usize; 2] {
        const { assert!(N > 0 && N < usize::MAX) };
        [N + 1; 2]
    }
}

// SAFETY: both axes are the exact rest count of paired entry/order units.
unsafe impl mem::raw::Amount<2, usize> for Remaining {
    fn get(self) -> [usize; 2] {
        [self.0.get(); 2]
    }
}

// SAFETY: expansion preserves the complete N-command-plus-terminal acquisition.
unsafe impl<const N: usize> mem::raw::ExactRebind<2, usize, Group<N>> for Expand {
    type Output = Remaining;

    fn rebind(self, _: Group<N>) -> Remaining {
        const { assert!(N > 0 && N < usize::MAX) };
        Remaining(NonZero::<usize>::MIN.saturating_add(N))
    }
}

// SAFETY: Group's const domain proves this expansion for every source value.
unsafe impl<const N: usize> mem::raw::Rebind<2, usize, Group<N>> for Expand {}

// SAFETY: split_group binds Take only for the N command positions of an N+1
// acquisition, so one terminal unit always remains after this exact partition.
unsafe impl mem::raw::Exact<2, usize, Remaining> for Take {
    type Input = ();
    type Left = Remaining;
    type Right = Units<1, 1>;

    fn parts(self, source: Remaining, (): ()) -> (Remaining, Units<1, 1>) {
        // SAFETY: the bound command position leaves at least the terminal unit.
        let rest = unsafe { NonZero::new_unchecked(source.0.get() - 1) };
        (Remaining(rest), Units::VALUE)
    }
}

// SAFETY: split_group binds Last only after all N command units were removed.
unsafe impl mem::raw::ExactRebind<2, usize, Remaining> for Last {
    type Output = Units<1, 1>;

    fn rebind(self, _: Remaining) -> Self::Output {
        Units::VALUE
    }
}

impl Entries {
    pub(in crate::reply) const EMPTY: Self = Self(0);

    /// Admits the entry units this exact amount carries.
    pub(in crate::reply) fn admit(self, amount: impl mem::raw::Amount<2, usize>) -> Self {
        Self(self.0 + amount.get()[0] as u32)
    }

    /// Releases the entry units this exact amount carries.
    pub(in crate::reply) fn release(self, amount: impl mem::raw::Amount<2, usize>) -> Self {
        Self(self.0 - amount.get()[0] as u32)
    }

    pub(in crate::reply) const fn get(self) -> usize {
        self.0 as usize
    }
}

impl<'credits, A: mem::raw::Amount<2, usize>> Grant<'credits, A> {
    pub(in crate::reply) const fn new(inner: Credit<'credits, A>) -> Self {
        Self(inner)
    }

    pub(in crate::reply) fn into_inner(self) -> Credit<'credits, A> {
        self.0
    }
}

impl<'credits, const N: usize> Grant<'credits, Group<N>> {
    pub(in crate::reply) fn split_group<T>(
        self,
        values: [T; N],
    ) -> (
        [(Grant<'credits, Units<1, 1>>, T); N],
        Grant<'credits, Units<1, 1>>,
    ) {
        let mut rest = self.0.map(Expand);
        let commands = values.map(|value| {
            // SAFETY: array construction visits exactly N distinct positions;
            // the N+1 acquisition therefore retains one terminal unit throughout.
            (Grant(unsafe { rest.bind_mut(Take) }.split(())), value)
        });
        // SAFETY: all N command units were transferred from this exact source.
        let terminal = Grant(unsafe { rest.bind(Last) }.map());
        (commands, terminal)
    }
}

impl Grant<'_, Units<0, 1>> {
    pub(in crate::reply) fn release(self) {
        self.0.release();
    }
}

// SAFETY: one entry and one order partition into their separate exact units.
unsafe impl mem::raw::Exact<2, usize, Units<1, 1>> for Units<1, 1> {
    type Input = ();
    type Left = Units<1, 0>;
    type Right = Units<0, 1>;

    fn parts(self, _: Self, (): ()) -> (Self::Left, Self::Right) {
        (Self::Left::VALUE, Self::Right::VALUE)
    }
}

impl<'credits, A> Grant<'credits, A>
where
    A: mem::raw::Amount<2, usize> + mem::raw::Exact<2, usize, A, Input = (), Right = Units<0, 1>>,
    A::Left: mem::raw::Amount<2, usize>,
{
    pub(in crate::reply) fn split(
        self,
    ) -> (Grant<'credits, A::Left>, Grant<'credits, Units<0, 1>>) {
        let amount = self.0.amount();
        // SAFETY: the supported unit amounts partition their complete source exactly;
        // the original amount itself carries this fixed-domain partition proof.
        let (left, order) = unsafe { self.0.bind(amount) }.split(());
        (Grant(left), Grant(order))
    }
}

const _: () = {
    assert!(size_of::<Grant<'_, Units<1, 0>>>() == size_of::<usize>());
    assert!(size_of::<Grant<'_, Units<0, 1>>>() == size_of::<usize>());
    assert!(size_of::<Grant<'_, Units<1, 1>>>() == size_of::<usize>());
};
