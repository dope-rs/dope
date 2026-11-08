//! Fiber-native bounded reply storage, ordering, and backpressure.

use std::{cell::OnceCell, marker, mem};

use o3::{
    alloc,
    base::cell::region,
    depot::{arena::linked, capacity, queue::pool::fifo},
    mem::rc,
};

use crate::reply::{
    arena::{backing::Backing, retire::Cell},
    recv::outcome::Completion,
    retain::pool,
    usage::Footprint,
};

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
pub use slot::Slot;
pub(crate) use slot::State;
pub(in crate::reply) use slot::{Drain, Failed, Failure, Item, Open};

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

/// Number of entries currently held by a reply arena or detached batch.
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

/// Immutable traversal of one detached reply batch.
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

struct Owned<'arena, 'credits, 'region, C: Footprint> {
    data: fifo::Nonempty<'arena, Item<C>, false>,
    credit: retain::Batch<'arena, 'credits, 'region>,
}

/// Fixed retirement storage for detached reply batches.
pub struct Batches<'arena, 'credits, 'region, C: Footprint> {
    queued: Cell<Owned<'arena, 'credits, 'region, C>>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

pub(in crate::reply) enum Advance {
    Idle,
    Dropped,
}

/// A linear, allocation-free owner of one completed reply's data chain.
pub struct Batch<'s, 'arena, 'credits, 'region, C: Footprint> {
    owned: o3::mem::slot::Storage<Owned<'arena, 'credits, 'region, C>>,
    retire: &'s Batches<'arena, 'credits, 'region, C>,
}

impl<'arena, 'credits, 'region, C: Footprint> Batches<'arena, 'credits, 'region, C> {
    pub(in crate::reply) fn try_new(capacity: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            queued: Cell::try_new(capacity)?,
            owner: marker::PhantomData,
        })
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.queued.is_empty()
    }

    /// Releases at most one physical item. The batch credit remains attached
    /// until the final node has been removed.
    pub(in crate::reply) fn advance(&self) -> Advance {
        let Some(owned) = self.queued.pop_front() else {
            return Advance::Idle;
        };
        if let Some(owned) = owned.retire() {
            self.queued.push(owned);
        }
        Advance::Dropped
    }

    /// Retains each admitted entry's credit until its final item retires,
    /// bounding this queue to one FIFO per entry in the exact arena.
    fn retire(&self, owned: Owned<'arena, 'credits, 'region, C>) {
        owned.credit.wake();
        self.queued.push(owned);
    }
}

impl<'s, 'arena, 'credits, 'region, C: Footprint> Batch<'s, 'arena, 'credits, 'region, C> {
    fn empty(retire: &'s Batches<'arena, 'credits, 'region, C>) -> Self {
        Self {
            owned: o3::mem::slot::Storage::new(()),
            retire,
        }
    }

    /// Detaches the complete data chain from one consumed completion capability.
    pub fn try_new<E>(
        complete: Completion<'_, 's, 'arena, 'credits, 'region, C, E>,
        retire: &'s Batches<'arena, 'credits, 'region, C>,
    ) -> Result<Self, rc::Error> {
        let Slot {
            state,
            owners,
            owner: _,
        } = complete.slot;
        let Some(front) = state.data.front() else {
            return Ok(Self::empty(retire));
        };
        let credit = owners.payload.try_batch()?;
        let data = front.take();
        Ok(Self {
            owned: o3::mem::slot::Storage::filled(Owned { data, credit }, ()),
            retire,
        })
    }

    pub fn len(&self) -> Length {
        self.owned
            .get()
            .map_or(Length::ZERO, |owned| Length(owned.credit.sum.len()))
    }

    pub fn usage(&self) -> usage::Usage {
        self.owned
            .get()
            .map_or(usage::Usage::ZERO, |owned| owned.credit.usage())
    }

    pub fn iter(&self) -> BatchIter<'_, C> {
        BatchIter::new(self.owned.get().map(|owned| owned.data.iter()))
    }
}

impl<C: Footprint> Drop for Batch<'_, '_, '_, '_, C> {
    fn drop(&mut self) {
        if let Some(owned) = self.owned.occupied() {
            self.retire.retire(owned.take().0);
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Length>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<OnceCell<Batches<'static, 'static, 'static, ()>>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Owned<'static, 'static, 'static, ()>>() == 7 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Owned<'static, 'static, 'static, ()>>>()
            == mem::size_of::<Owned<'static, 'static, 'static, ()>>()
    );
    assert!(
        mem::size_of::<Batch<'static, 'static, 'static, 'static, ()>>()
            == 8 * mem::size_of::<usize>()
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

    pub fn try_batches<E, K, R>(
        self,
        run: impl for<'scope> FnOnce(
            &'scope Arena<'arena, 'credits, 'region, D, E, K>,
            &'scope Batches<'arena, 'credits, 'region, D>,
        ) -> R,
    ) -> Result<R, alloc::Error> {
        let batches = Batches::try_new(capacity::Count::new(self.layout.entries().get()))?;
        let arena = Arena::try_new(self.backing, self.layout, self.proof)?;
        let batches = arena.batches.get_or_init(|| batches);
        Ok(run(&arena, batches))
    }
}
