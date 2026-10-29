//! Typed views over one reply slot.

use std::{marker::PhantomData, mem};

use o3::{
    depot::{self, arena::linked::slots, queue::pool::fifo},
    mem::rc,
};

use crate::reply::{
    arena::state::{self, order, order::Lease},
    layout,
    order::Depth,
    recv::{self, outcome},
    retain::{
        self,
        grant::payload,
        pool::{Acquire, Owner},
    },
    usage,
};

mod end;

use end::End;
pub(in crate::reply) use end::Fault;

pub(in crate::reply) struct Item<C> {
    pub(in crate::reply) value: C,
    pub(in crate::reply) amount: payload::Value,
}

pub(in crate::reply) struct Failed<C> {
    pub(in crate::reply) value: C,
    pub(in crate::reply) amount: payload::Failure,
}

#[repr(C)]
pub(crate) struct State<'arena, D, E> {
    end: End<E>,
    pub(in crate::reply) data: fifo::Fifo<'arena, Item<D>, false>,
}

pub(in crate::reply) struct Failure<'a, E> {
    end: &'a mut End<E>,
}

/// The exact front order entry and its attached open reply slot.
pub(in crate::reply) struct Open<'a, 'arena, 'credits, 'region, D, E, K = ()> {
    order: order::Slot<'a, 'arena, 'credits, 'region, K>,
    acceptance: recv::Acceptance,
    state: &'a mut State<'arena, D, E>,
    signal: &'a mut state::Signal<'region>,
}

pub(in crate::reply) enum Drain {
    Empty,
    Dropped,
}

impl<'arena, D, E> State<'arena, D, E> {
    pub(in crate::reply) fn new(data: fifo::Fifo<'arena, Item<D>, false>) -> Self {
        Self {
            end: End::Open,
            data,
        }
    }

    pub(in crate::reply) fn take_data(&mut self) -> Option<Item<D>> {
        self.data.pop_front()
    }

    pub(in crate::reply) fn take_failure(&mut self) -> Option<Failed<E>> {
        self.end.take_failure()
    }

    pub(in crate::reply) fn is_finished(&self) -> bool {
        matches!(self.end, End::Complete) && self.data.is_empty()
    }
}

impl<'a, 'arena, 'credits, 'region, D, E, K> Open<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply) const fn new(
        order: order::Slot<'a, 'arena, 'credits, 'region, K>,
        accept: recv::Acceptance,
        state: &'a mut State<'arena, D, E>,
        signal: &'a mut state::Signal<'region>,
    ) -> Self {
        Self {
            order,
            acceptance: accept,
            state,
            signal,
        }
    }

    pub(in crate::reply) const fn acceptance(&self) -> recv::Acceptance {
        self.acceptance
    }

    fn try_usage(&self, limit: layout::Limit, added: usage::Usage) -> Result<(), usage::Capacity> {
        let current = self.order.usage();
        let next = current.try_add(added)?;
        if next.bytes() > limit.bytes() {
            return Err(usage::Capacity::Limit {
                resource: usage::Resource::Bytes,
                current: usage::Current::new(current.bytes().get()),
                added: usage::Added::new(added.bytes().get()),
                limit: usage::Limit::new(limit.bytes().get()),
            });
        }
        if next.values() > limit.values() {
            return Err(usage::Capacity::Limit {
                resource: usage::Resource::Values,
                current: usage::Current::new(current.values().get()),
                added: usage::Added::new(added.values().get()),
                limit: usage::Limit::new(limit.values().get()),
            });
        }
        Ok(())
    }

    pub(in crate::reply) fn try_data(
        self,
        limit: layout::Limit,
        added: usage::Usage,
    ) -> Result<Push<'a, 'arena, 'credits, 'region, D, K>, Acquire> {
        let current = self.order.len();
        if current >= limit.data().get() {
            return Err(Acquire::Capacity(usage::Capacity::Limit {
                resource: usage::Resource::Items,
                current: usage::Current::new(current),
                added: usage::Added::new(1),
                limit: usage::Limit::new(limit.data().get()),
            }));
        }
        self.try_usage(limit, added).map_err(Acquire::Capacity)?;
        let lease = self.order.try_lease(payload::Value::new(added))?;
        Ok(fifo::Fifo::reserve(Reserve {
            queue: &mut self.state.data,
            lease,
            signal: self.signal,
        }))
    }

    pub(in crate::reply) fn try_failure(
        self,
        limit: layout::Limit,
        added: usage::Usage,
    ) -> Result<
        (
            order::Slot<'a, 'arena, 'credits, 'region, K>,
            Failure<'a, E>,
            &'a mut state::Signal<'region>,
        ),
        usage::Capacity,
    > {
        self.try_usage(limit, added)?;
        Ok((
            self.order,
            Failure {
                end: &mut self.state.end,
            },
            self.signal,
        ))
    }

    pub(in crate::reply) fn complete(self) -> Depth {
        self.state.end = End::Complete;
        let depth = self.order.cancel_pop();
        self.signal.notify();
        depth
    }

    pub(in crate::reply) fn fail_pop<'cause, C>(self, cause: &'cause C) -> Depth
    where
        E: From<&'cause C>,
    {
        self.state.end.fail(Failed {
            value: E::from(cause),
            amount: payload::Failure::empty(),
        });
        let depth = self.order.cancel_pop();
        self.signal.notify();
        depth
    }
}

impl<E> Failure<'_, E> {
    pub(in crate::reply) fn commit(self, item: Failed<E>) {
        self.end.fail(item);
    }
}

/// Mutable reply-slot view that prevents arena reentry while borrowed.
pub struct Slot<'a, 's, 'arena, 'credits, 'region, D, E = D> {
    pub(in crate::reply) state: &'a mut State<'arena, D, E>,
    pub(in crate::reply) owners: &'a mut Owner<'arena, 'credits, 'region>,
    pub(in crate::reply) owner: PhantomData<&'s ()>,
}

impl<'s, 'arena, 'credits, 'region, D, E> Slot<'_, 's, 'arena, 'credits, 'region, D, E> {
    pub fn terminal(
        &mut self,
    ) -> Option<outcome::Terminal<'_, 's, 'arena, 'credits, 'region, D, E>> {
        let slot = Slot {
            state: &mut *self.state,
            owners: &mut *self.owners,
            owner: self.owner,
        };
        Fault::observe(slot)
    }

    /// Moves one data item after securing its independent accounting edge.
    pub fn try_lease(
        &mut self,
    ) -> Result<Option<retain::Lease<'arena, 'credits, 'region, D>>, rc::Error> {
        let Some(front) = self.state.data.front() else {
            return Ok(None);
        };
        // SAFETY: this front and owner were loaned from the same occupied slot;
        // its immutable original amount remains a live member until front.pop.
        let claim =
            unsafe { retain::raw::Remove::remove(&mut self.owners.payload, front.get().amount) }
                .try_value()?;
        Ok(Some(retain::Lease::new(front.pop().value, claim)))
    }

    pub fn pop_error(&mut self) -> Option<E> {
        let item = self.state.take_failure()?;
        // SAFETY: the removed member and payload come from this exact owned slot.
        unsafe { retain::raw::Remove::remove(&mut self.owners.payload, item.amount) }.release();
        Some(item.value)
    }

    pub fn pop(&mut self) -> Option<D> {
        let item = self.state.take_data()?;
        // SAFETY: the removed member and payload come from this exact owned slot.
        unsafe { retain::raw::Remove::remove(&mut self.owners.payload, item.amount) }.release();
        Some(item.value)
    }
}

const _: () = {
    assert!(mem::size_of::<Item<u64>>() == mem::size_of::<(u64, usage::Usage)>());
    assert!(mem::size_of::<Failed<u64>>() == mem::size_of::<(u64, usage::Usage)>());
    assert!(mem::size_of::<State<'static, u64, u64>>() == 6 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<State<'static, u64, u64>>()
            + mem::size_of::<Owner<'static, 'static, 'static>>()
            == 14 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Open<'static, 'static, 'static, 'static, (), ()>>()
            == 8 * mem::size_of::<usize>()
    );
};

#[must_use]
pub(in crate::reply) struct Push<'a, 'arena, 'credits, 'region, C, K = ()> {
    entry: slots::Vacant<Item<C>, &'a mut fifo::Fifo<'arena, Item<C>, false>>,
    lease: Lease<'a, 'arena, 'credits, 'region, payload::Value, K>,
    signal: &'a mut state::Signal<'region>,
}

struct Reserve<'a, 'arena, 'credits, 'region, C, K> {
    queue: &'a mut fifo::Fifo<'arena, Item<C>, false>,
    lease: Lease<'a, 'arena, 'credits, 'region, payload::Value, K>,
    signal: &'a mut state::Signal<'region>,
}

// SAFETY: try_data and try_lease acquire one unused payload slot from this
// arena's exact pool. The affine lease stays paired with its reserved entry
// until both are committed or returned, including an abandoned Push. The signal
// remains borrowed from that same reply entry through publication.
unsafe impl<'a, 'arena: 'a, 'credits, 'region, C: 'arena, K> depot::raw::Reserve<'a, 'arena, false>
    for Reserve<'a, 'arena, 'credits, 'region, C, K>
{
    type Item = Item<C>;
    type Credit = (
        Lease<'a, 'arena, 'credits, 'region, payload::Value, K>,
        &'a mut state::Signal<'region>,
    );
    type Reserved = Push<'a, 'arena, 'credits, 'region, C, K>;

    /// # Safety
    /// The caller retains the reservation these exact parts were built from.
    unsafe fn into_parts(self) -> (&'a mut fifo::Fifo<'arena, Self::Item, false>, Self::Credit) {
        (self.queue, (self.lease, self.signal))
    }
    /// # Safety
    /// `entry` must be the vacant slot of this exact reserved queue position.
    unsafe fn build(
        entry: slots::Vacant<Self::Item, &'a mut fifo::Fifo<'arena, Self::Item, false>>,
        (lease, signal): Self::Credit,
    ) -> Self::Reserved {
        Push {
            entry,
            lease,
            signal,
        }
    }
}

impl<'a, 'arena, 'credits, 'region, C, K> Push<'a, 'arena, 'credits, 'region, C, K> {
    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        self.lease.usage()
    }

    pub(in crate::reply) fn commit(self, value: C) {
        let amount = self.lease.amount();
        self.lease.keep();
        self.entry.push_back(Item { value, amount });
        self.signal.notify();
    }
}

const _: () = {
    assert!(
        mem::size_of::<Push<'static, 'static, 'static, 'static, u64>>()
            == 10 * mem::size_of::<usize>()
    );
};
