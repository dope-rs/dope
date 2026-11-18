//! Complete per-entry reply-pool ownership.

use std::mem::size_of;

use o3::mem::{self, rc};

use crate::reply::{
    LaneIndex, Length,
    arena::{retire, state::Index},
    order, recv,
    retain::{
        self,
        grant::{entries, payload},
        pool::{self, Acquire, Active, Key, LeaseEdge, Release},
    },
    usage,
};

pub(in crate::reply) struct Owner<'arena, 'credits, 'region> {
    order: order::State,
    pub(in crate::reply) acceptance: Option<recv::Acceptance>,
    pub(in crate::reply) payload: pool::Payload<'arena, 'credits, 'region>,
}

pub(in crate::reply) struct Payload<'arena, 'credits, 'region> {
    key: Key<'arena, 'credits, 'region>,
    lane: LaneIndex,
    sum: payload::raw::Sum,
    source: &'credits retain::raw::State,
}

/// One unchanged member and its source owner, retained until removal commits.
pub(in crate::reply) struct Removal<'a, 'arena, 'credits, 'region, A: mem::raw::Amount<3, usize>> {
    owner: &'a mut Payload<'arena, 'credits, 'region>,
    amount: A,
}

impl<'arena, 'credits, 'region> Owner<'arena, 'credits, 'region> {
    #[inline]
    pub(in crate::reply) fn new(
        admitted: pool::Registration<'arena, 'credits, 'region, entries::Unit>,
        entry_index: Index,
        acceptance: recv::Acceptance,
    ) -> Self {
        let (lane, source) = admitted.into_parts();
        let pool = lane.pool();
        let index = lane.index();
        let sum = payload::raw::Sum::new();
        let key = pool.bind(entry_index, source);
        Self {
            order: order::State::Linked,
            acceptance: Some(acceptance),
            payload: Payload {
                key,
                lane: index,
                sum,
                source,
            },
        }
    }

    pub(in crate::reply) const fn order(&self) -> order::State {
        self.order
    }

    pub(in crate::reply) fn clear_order(&mut self) {
        self.order = order::State::Detached;
    }

    pub(in crate::reply) fn retire(&self) {
        self.payload.key.retire();
    }

    pub(in crate::reply) fn cancel_entry(&self) {
        self.payload.key.cancel_entry(self.payload.lane);
    }

    pub(in crate::reply) fn is_empty(&self) -> bool {
        matches!(self.order, order::State::Detached)
            && self.payload.sum.len() == 0
            && self.payload.sum.usage() == usage::Usage::ZERO
            && self.payload.key.state().life.is_vacant()
    }

    pub(in crate::reply) const fn lane(&self) -> LaneIndex {
        self.payload.lane
    }

    pub(in crate::reply) fn unbind(self) -> (LaneIndex, Release) {
        let lane = self.payload.lane;
        let release = self.payload.release_slot();
        (lane, release)
    }
}

impl<'arena, 'credits, 'region> Payload<'arena, 'credits, 'region> {
    pub(in crate::reply) fn len(&self) -> Length {
        Length::new(self.sum.len())
    }

    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        self.sum.usage()
    }

    pub(in crate::reply) fn defer_retire(&self, retired: &retire::State) {
        retired.push(retire::Retired::new(self.key.state().index()));
        self.key.parts().core.lanes.wake(self.lane);
    }

    fn release_slot(self) -> Release {
        let parts = self.key.parts();
        let release = if self.source.is_reserved() {
            Release::Reserved
        } else {
            Release::Shared
        };
        // SAFETY: Registration bound this source before publishing the entry;
        // consuming its Payload returns that original counted admission once.
        unsafe { parts.core.lanes.credits.release_slot(self.source) };
        release
    }

    fn activate(&self) -> Active<'arena, 'credits, 'region> {
        self.key.activate(self.lane)
    }

    pub(in crate::reply) fn try_acquire<
        A: mem::raw::Amount<3, usize> + Into<payload::Requested>,
    >(
        &mut self,
        amount: A,
        acceptance: recv::Acceptance,
    ) -> Result<(), Acquire> {
        let core = self.key.parts().core;
        let lanes = &core.lanes;
        let limit = core.limit;
        let key = self.key.state().index();
        let [items, values, bytes] = amount.get();
        let current = self.sum.usage();
        let next = current
            .try_add(usage::Usage::new(
                usage::Bytes::new(bytes),
                usage::Values::new(values),
            ))
            .map_err(|cause| {
                lanes.cancel_entry(self.lane, key);
                Acquire::Capacity(cause)
            })?;
        let held = self.sum.len();
        let mut blocked = None;
        for (resource, current, added, limit, exceeded) in [
            (
                usage::Resource::Items,
                held,
                items,
                limit.data().get(),
                items > limit.data().get() - held,
            ),
            (
                usage::Resource::Bytes,
                current.bytes().get(),
                bytes,
                limit.bytes().get(),
                next.bytes() > limit.bytes(),
            ),
            (
                usage::Resource::Values,
                current.values().get(),
                values,
                limit.values().get(),
                next.values() > limit.values(),
            ),
        ] {
            if !exceeded {
                continue;
            }
            if acceptance != recv::Acceptance::Payload || added > limit {
                lanes.cancel_entry(self.lane, key);
                return Err(Acquire::Capacity(usage::Capacity::Limit {
                    resource,
                    current: usage::Current::new(current),
                    added: usage::Added::new(added),
                    limit: usage::Limit::new(limit),
                }));
            }
            if blocked.is_none() {
                blocked = Some(usage::Backpressure::new(
                    resource,
                    usage::Required::new(added),
                    usage::Available::new(limit - current),
                ));
            }
        }
        if let Some(cause) = blocked {
            lanes.wait_local(self.lane, key);
            return Err(Acquire::Backpressure(cause));
        }
        lanes.take_waiting(self.lane, key, self.source, amount, acceptance)?;
        // SAFETY: the same State just admitted this disjoint member; its checked
        // total bounds the sum, and no user code runs before this commit.
        unsafe { self.sum.add(amount) };
        Ok(())
    }

    fn release_data<A: mem::raw::Amount<3, usize>>(&self, amount: A) {
        self.return_data(amount);
        let lanes = &self.key.parts().core.lanes;
        lanes.wake_local(self.lane, self.key.state().index());
    }

    fn return_data<A: mem::raw::Amount<3, usize>>(&self, amount: A) {
        // SAFETY: callers consume original members from this unchanged source.
        unsafe {
            self.key
                .parts()
                .core
                .lanes
                .release_data(self.source, amount)
        };
    }

    /// # Safety
    /// The original first node is detached, and its sole root owns this member.
    pub(in crate::reply) unsafe fn take_first(&mut self, amount: payload::Value<usage::Bytes>) {
        // SAFETY: the caller retains this exact member until its root takes custody.
        unsafe { self.sum.take(amount) };
        self.return_data(payload::Data);
    }

    /// # Safety
    /// The original first node has been freed; its retained amount returns once.
    pub(in crate::reply) unsafe fn release_first(&self, amount: payload::Failure<usage::Bytes>) {
        self.return_data(amount);
    }

    fn try_lease<A: mem::raw::Amount<3, usize>>(
        &self,
        amount: A,
    ) -> Result<mem::Lease<LeaseEdge<'arena, 'credits, 'region, A>>, rc::Error> {
        let edge = LeaseEdge::try_new(self.activate(), self.source)?;
        Ok(mem::Lease::new(edge, amount))
    }

    fn try_hold(
        &self,
        amount: payload::Failure,
    ) -> Result<retain::Held<'arena, 'credits, 'region>, rc::Error> {
        Ok(match amount.split() {
            payload::Class::Zero => retain::Held::empty(),
            payload::Class::Values(amount) => {
                let active = self.activate();
                let credit = active.try_values()?;
                // SAFETY: the new edge and unchanged member share this entry's source.
                unsafe { active.state().values.activate(amount) };
                retain::Held {
                    bytes: None,
                    values: Some(credit),
                }
            }
            payload::Class::Bytes(amount) => {
                let active = self.activate();
                let credit = active.try_bytes()?;
                // SAFETY: the new edge and unchanged member share this entry's source.
                unsafe { active.state().bytes.activate(amount) };
                retain::Held {
                    bytes: Some(credit),
                    values: None,
                }
            }
            payload::Class::Both(values, bytes) => {
                let active = self.activate();
                let held = retain::Held {
                    values: Some(active.try_values()?),
                    bytes: Some(active.try_bytes()?),
                };
                // SAFETY: both holds succeeded before either amount is transferred.
                unsafe {
                    active.state().values.activate(values);
                    active.state().bytes.activate(bytes);
                }
                held
            }
        })
    }

    /// # Safety
    /// The caller owns this original member's still-accounted amount.
    /// That amount must not have been removed from this sum before.
    pub(in crate::reply) unsafe fn remove<A>(
        &mut self,
        amount: A,
    ) -> Removal<'_, 'arena, 'credits, 'region, A>
    where
        A: mem::raw::Amount<3, usize>,
    {
        Removal {
            owner: self,
            amount,
        }
    }
}

impl<A: mem::raw::Amount<3, usize>> Removal<'_, '_, '_, '_, A> {
    pub(in crate::reply) fn release(self) {
        // SAFETY: Removal retains the original unremoved member and its sum.
        let amount = unsafe { self.owner.sum.take(self.amount) };
        self.owner.release_data(amount);
    }

    pub(in crate::reply) fn retire(self) {
        // SAFETY: terminal/retiring owners destroyed the payload and retain its
        // original accounted amount; the exact entry's local waiter is removed.
        let amount = unsafe { self.owner.sum.take(self.amount) };
        self.owner.return_data(amount);
    }
}

impl<'arena, 'credits, 'region> Removal<'_, 'arena, 'credits, 'region, payload::Value> {
    pub(in crate::reply) fn try_retain<T>(
        self,
        before: impl FnOnce(),
        take: impl FnOnce() -> T,
    ) -> Result<retain::Retained<'arena, 'credits, 'region, T>, rc::Error> {
        let Self { owner, amount } = self;
        let (data, retained) = amount.split();
        let held = owner.try_hold(retained);
        before();
        let held = held?;
        let value = take();
        // SAFETY: every hold succeeded; this unchanged member now transfers once.
        unsafe { owner.sum.take(amount) };
        owner.release_data(data);
        Ok(retain::Retained::new(value, held))
    }

    pub(in crate::reply) fn try_value(
        self,
    ) -> Result<Option<mem::Lease<LeaseEdge<'arena, 'credits, 'region>>>, rc::Error> {
        let Self { owner, amount } = self;
        let (data, retained) = amount.split();
        let lease = match retained.claim() {
            None => None,
            Some(retained) => Some(owner.try_lease(retained)?),
        };
        // SAFETY: the lease was admitted without removing this original member.
        unsafe { owner.sum.take(amount) };
        owner.release_data(data);
        Ok(lease)
    }
}

impl<'arena, 'credits, 'region>
    Removal<'_, 'arena, 'credits, 'region, payload::Value<usage::Bytes>>
{
    #[inline]
    pub(in crate::reply) fn try_value(
        self,
    ) -> Result<
        mem::Lease<LeaseEdge<'arena, 'credits, 'region, payload::Failure<usage::Bytes>>>,
        rc::Error,
    > {
        let Self { owner, amount } = self;
        let (data, retained) = amount.split();
        let lease = owner.try_lease(retained)?;
        // SAFETY: the lease was admitted without removing this original member.
        unsafe { owner.sum.take(amount) };
        owner.release_data(data);
        Ok(lease)
    }
}

const _: () = {
    assert!(size_of::<Payload<'static, 'static, 'static>>() == 6 * size_of::<usize>());
    assert!(size_of::<Owner<'static, 'static, 'static>>() == size_of::<[usize; 7]>());
};
