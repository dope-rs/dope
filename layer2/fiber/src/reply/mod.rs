//! Fiber-native bounded reply storage, ordering, and backpressure.

use std::{marker, mem};

use o3::{alloc, base::cell::region, depot::arena::linked};

use crate::reply::{arena::backing::Backing, retain::pool, usage::Footprint};

mod arena;
pub mod layout;
pub mod order;
pub(crate) mod raw;
pub mod recv;
pub mod retain;
mod slot;
pub mod usage;

pub use arena::{
    Arena,
    control::Control,
    lanes::{Error, Lane, LaneIndex, Released, Retirement},
    progress::{Progress, Work},
    state::Push,
};
pub(crate) use slot::State;
pub(in crate::reply) use slot::{Failed, Item, Open};
pub use slot::{Failure, Slot};

/// Exact capacity state for registering one reply entry.
#[must_use = "registration state must be consumed"]
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Registration {
    Available,
    Full,
    IdentityExhausted,
}

const _: () = assert!(std::mem::size_of::<Registration>() == std::mem::size_of::<u8>());

/// Number of entries currently held by a reply arena or completed batch.
#[derive(Clone, Copy, Default, Eq, Ord, PartialEq, PartialOrd, Debug)]
#[repr(transparent)]
pub struct Length(pub(in crate::reply) usize);

impl Length {
    pub const ZERO: Self = Self(0);

    pub(in crate::reply) const fn new(entries: usize) -> Self {
        Self(entries)
    }

    /// Returns the exact entry count for presentation or sizing.
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Immutable traversal of one completed reply batch.
pub struct BatchIter<'batch, C: Footprint> {
    inner: Option<linked::Iter<'batch, Item<C>>>,
}

impl<'batch, C: Footprint> BatchIter<'batch, C> {
    pub(in crate::reply) const fn new(inner: Option<linked::Iter<'batch, Item<C>>>) -> Self {
        Self { inner }
    }
}

impl<'batch, C: Footprint> Iterator for BatchIter<'batch, C> {
    type Item = &'batch C;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.as_mut()?.next()?;
        Some(&item.value)
    }
}

/// A linear, allocation-free owner of one completed reply's data chain.
pub struct Batch<'s, 'arena, 'credits, 'region, C: Footprint> {
    contents: Option<raw::Terminal<'s, 'arena, 'credits, 'region, C>>,
}

impl<'s, 'arena, 'credits, 'region, C: Footprint> Batch<'s, 'arena, 'credits, 'region, C> {
    pub fn len(&self) -> Length {
        self.contents
            .as_ref()
            .map_or(Length::ZERO, raw::Terminal::len)
    }

    pub fn usage(&self) -> usage::Usage {
        self.contents
            .as_ref()
            .map_or(usage::Usage::ZERO, raw::Terminal::usage)
    }

    pub fn iter(&self) -> BatchIter<'_, C> {
        BatchIter::new(self.contents.as_ref().map(raw::Terminal::iter))
    }
}

const _: () = {
    assert!(mem::size_of::<Length>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Batch<'static, 'static, 'static, 'static, ()>>()
            == 3 * mem::size_of::<usize>()
    );
};

/// The original reply backing scope, before its tagged arena is constructed.
pub struct Storage<'arena, 'credits, 'region, D: Footprint> {
    backing: &'arena Backing<'credits, 'region, D>,
    layout: layout::Layout,
    proof: region::Identity<'region>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

impl<'region, D: Footprint> Storage<'_, '_, 'region, D> {
    /// Lends the accounting and data backing before constructing owner-bearing tags.
    pub fn try_with<R>(
        layout: layout::Layout,
        proof: region::Identity<'region>,
        run: impl for<'arena, 'credits> FnOnce(Storage<'arena, 'credits, 'region, D>) -> R,
    ) -> Result<R, alloc::Error> {
        let slots = pool::Slots::try_new(layout)?;
        let backing = Backing::try_new(layout, &slots)?;
        Ok(run(Storage {
            backing: &backing,
            layout,
            proof,
            owner: marker::PhantomData,
        }))
    }
}

impl<'arena, 'credits, 'region, D: Footprint> Storage<'arena, 'credits, 'region, D> {
    pub fn try_arena<E, K, R>(
        self,
        run: impl for<'scope> FnOnce(&'scope Arena<'arena, 'credits, 'region, D, E, K>) -> R,
    ) -> Result<R, alloc::Error> {
        let arena = Arena::try_new(self.backing, self.layout, self.proof)?;
        Ok(run(&arena))
    }
}
