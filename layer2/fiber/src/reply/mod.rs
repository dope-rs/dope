//! Fiber-native bounded reply storage, ordering, and backpressure.

use std::{cell::OnceCell, marker, mem};

use o3::{
    alloc::{self, array},
    base::cell::region,
    depot::{arena::linked, capacity, queue::pool::fifo},
    mem::rc,
};

use crate::reply::{
    arena::{backing::Backing, retire::Cell},
    recv::outcome::Completion,
    retain::pool,
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
pub(in crate::reply) use slot::{Drain, Failed, Failure, Fault, Item, Open};

/// Exact attachment state of one reply lane.
#[must_use = "attachment state must be consumed"]
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Attachment {
    Detached,
    Attached,
}

const _: () = assert!(std::mem::size_of::<Attachment>() == std::mem::size_of::<u8>());

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
pub struct BatchIter<'batch, C> {
    inner: Option<linked::Iter<'batch, Item<C>>>,
}

impl<'batch, C> BatchIter<'batch, C> {
    pub(in crate::reply) const fn new(inner: Option<linked::Iter<'batch, Item<C>>>) -> Self {
        Self { inner }
    }
}

impl<'batch, C> Iterator for BatchIter<'batch, C> {
    type Item = &'batch C;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.as_mut()?.next()?;
        Some(&item.value)
    }
}

struct Owned<'arena, 'credits, 'region, C> {
    data: fifo::Nonempty<'arena, Item<C>, false>,
    credit: retain::Batch<'arena, 'credits, 'region>,
}

impl<C> Owned<'_, '_, '_, C> {
    fn retire(self) -> Option<Self> {
        let Self { data, mut credit } = self;
        let data = {
            let (item, next) = data.pop();
            // SAFETY: try_new captures this completed slot's entire FIFO and sum
            // together. This private owner removes each original member once and
            // never accepts an independently supplied item or credit.
            let part = unsafe { credit.sum.take(item.amount) };
            part.into_inner().release();
            credit.returned(item.amount);
            next
        };
        Some(Self {
            data: data?,
            credit,
        })
    }
}

struct Retired<'arena, 'credits, 'region, C>(Owned<'arena, 'credits, 'region, C>);

/// Fixed retirement storage for detached reply batches.
pub struct Batches<'arena, 'credits, 'region, C> {
    queued: Cell<Retired<'arena, 'credits, 'region, C>>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

pub(in crate::reply) enum Advance {
    Idle,
    Dropped,
}

/// A linear, allocation-free owner of one completed reply's data chain.
pub struct Batch<'s, 'arena, 'credits, 'region, C> {
    owned: Option<Owned<'arena, 'credits, 'region, C>>,
    retire: &'s Batches<'arena, 'credits, 'region, C>,
}

impl<'arena, 'credits, 'region, C> Batches<'arena, 'credits, 'region, C> {
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
        let Some(retired) = self.queued.pop_front() else {
            return Advance::Idle;
        };
        if let Some(owned) = retired.0.retire() {
            self.queued.push(Retired(owned));
        }
        Advance::Dropped
    }

    /// Retains each admitted entry's credit until its final item retires,
    /// bounding this queue to one FIFO per entry in the exact arena.
    fn retire(&self, owned: Owned<'arena, 'credits, 'region, C>) {
        owned.credit.wake();
        self.queued.push(Retired(owned));
    }
}

impl<'s, 'arena, 'credits, 'region, C> Batch<'s, 'arena, 'credits, 'region, C> {
    fn empty(retire: &'s Batches<'arena, 'credits, 'region, C>) -> Self {
        Self {
            owned: None,
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
            owned: Some(Owned { data, credit }),
            retire,
        })
    }

    pub fn len(&self) -> Length {
        self.owned
            .as_ref()
            .map_or(Length::ZERO, |owned| Length(owned.credit.sum.len()))
    }

    pub fn usage(&self) -> usage::Usage {
        self.owned
            .as_ref()
            .map_or(usage::Usage::ZERO, |owned| owned.credit.usage())
    }

    pub fn iter(&self) -> BatchIter<'_, C> {
        BatchIter::new(self.owned.as_ref().map(|owned| owned.data.iter()))
    }

    /// Maps this retained batch into one exactly sized allocation. Each slot's
    /// consuming insertion commits its value before further callback work.
    /// Allocation and callback errors stay separate; this borrow is not retired.
    pub fn try_map<T, E>(
        &self,
        make: impl for<'slot> FnMut(&C, array::Slot<'slot, T>) -> Result<array::Written<'slot>, E>,
    ) -> Result<Result<Vec<T>, E>, alloc::Error> {
        raw::try_map(self, make)
    }
}

impl<C> Drop for Batch<'_, '_, '_, '_, C> {
    fn drop(&mut self) {
        if let Some(owned) = self.owned.take() {
            self.retire.retire(owned);
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Length>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<OnceCell<Batches<'static, 'static, 'static, u64>>>()
            == 3 * mem::size_of::<usize>()
    );
    assert!(mem::size_of::<Owned<'static, 'static, 'static, u64>>() == 7 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Owned<'static, 'static, 'static, u64>>>()
            == mem::size_of::<Owned<'static, 'static, 'static, u64>>()
    );
    assert!(
        mem::size_of::<Batch<'static, 'static, 'static, 'static, u64>>()
            == 8 * mem::size_of::<usize>()
    );
};

/// The original reply backing scope, before its tagged arena is constructed.
pub struct Storage<'arena, 'credits, 'region, D> {
    backing: &'arena Backing<'credits, 'region, D>,
    layout: layout::Layout,
    proof: region::Proof<region::Identity<'region>>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

impl<'region, D> Storage<'_, '_, 'region, D> {
    /// Lends the accounting and data backing before constructing owner-bearing tags.
    pub fn try_with<R>(
        layout: layout::Layout,
        proof: region::Proof<region::Identity<'region>>,
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

impl<'arena, 'credits, 'region, D> Storage<'arena, 'credits, 'region, D> {
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
