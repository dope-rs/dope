//! Complete per-entry reply-pool ownership.

use std::mem::size_of;

use o3::mem::{self, raw::Source as _, rc};

use crate::reply::{
    LaneIndex,
    arena::state::Index,
    layout, order, recv,
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
    sum: payload::raw::Sum<'credits>,
    entry: entries::Grant<'credits, entries::Unit>,
}

/// One removed member and its exact source owner, kept inseparable until
/// retention commits or the original acquisition is returned.
pub(in crate::reply) struct Removal<'a, 'arena, 'credits, 'region, A: mem::raw::Amount<3, usize>> {
    owner: &'a mut Payload<'arena, 'credits, 'region>,
    grant: payload::Grant<'credits, A>,
}

impl<'arena, 'credits, 'region> Owner<'arena, 'credits, 'region> {
    #[inline]
    pub(in crate::reply) fn new(
        admitted: pool::Registration<'arena, 'credits, 'region, entries::Unit>,
        entry_index: Index,
        acceptance: recv::Acceptance,
    ) -> Self {
        let (lane, entry) = admitted.into_parts();
        let pool = lane.pool();
        let index = lane.index();
        let sum = payload::raw::Sum::new(pool.lanes().credits.payload(index));
        let key = pool.bind(entry_index);
        Self {
            order: order::State::Linked,
            acceptance: Some(acceptance),
            payload: Payload {
                key,
                lane: index,
                sum,
                entry,
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
    fn release_slot(self) -> Release {
        let parts = self.key.parts();
        let credit = parts.core.lanes.credits.capacity(self.lane);
        let release = if credit.held()[0] == credit.reserve()[0] {
            Release::Reserved
        } else {
            Release::Shared
        };
        // SAFETY: Registration minted the original lane and acquisition together;
        // Owner stores their key and grant in this private Payload without any
        // independent rebinding API. Consuming it returns that exact count once.
        unsafe { pool::raw::Release::release_slot(&parts.core.lanes.credits, self.entry) };
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
        limit: layout::Limit,
        acceptance: recv::Acceptance,
    ) -> Result<(), Acquire> {
        let lanes = &self.key.parts().core.lanes;
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
        lanes.take_waiting(self.lane, key, &mut self.sum, amount, acceptance)
    }

    fn restore<A: mem::raw::Amount<3, usize>>(&mut self, grant: payload::Grant<'credits, A>) {
        // SAFETY: only Removal's rollback returns pieces of its captured source.
        unsafe { self.sum.merge(grant) };
    }

    fn try_lease<A: mem::raw::Amount<3, usize>>(
        &mut self,
        data: payload::Grant<'credits, payload::Data>,
        grant: payload::Grant<'credits, A>,
    ) -> Result<mem::Lease<LeaseEdge<'arena, 'credits, 'region, A>>, rc::Error> {
        let edge = match LeaseEdge::try_new(self.activate()) {
            Ok(edge) => edge,
            Err(error) => {
                self.restore(data);
                self.restore(grant);
                return Err(error);
            }
        };
        self.key.release_data(self.lane, data);
        Ok(mem::Lease::new(edge, grant))
    }

    pub(in crate::reply) fn try_batch(
        &mut self,
    ) -> Result<retain::Batch<'arena, 'credits, 'region>, rc::Error> {
        Ok(retain::Batch {
            edge: LeaseEdge::try_new(self.activate())?,
            sum: self.sum.drain(),
        })
    }
}

// SAFETY: the implementation preserves the caller's original-member obligation
// and captures the exact exclusive owner in every resulting Removal.
unsafe impl<'arena, 'credits, 'region> retain::raw::Remove<'arena, 'credits, 'region>
    for Payload<'arena, 'credits, 'region>
{
    /// # Safety
    /// The caller must own the exact original slot member represented by amount.
    /// That member must not have been removed from this sum before.
    unsafe fn remove<A>(&mut self, amount: A) -> Removal<'_, 'arena, 'credits, 'region, A>
    where
        A: mem::raw::Amount<3, usize>,
    {
        // SAFETY: the owning slot/removal proves membership in this exact source;
        // take splits it under the exclusive borrow, then Removal retains it.
        let grant = unsafe { self.sum.take(amount) };
        Removal { owner: self, grant }
    }
}

impl<'arena, 'credits, 'region> Removal<'_, 'arena, 'credits, 'region, payload::Failure> {
    fn try_hold(self) -> Result<retain::Held<'arena, 'credits, 'region>, rc::Error> {
        let Self { owner, grant } = self;
        let held = match payload::Failure::split(grant) {
            payload::Class::Zero => retain::Held::empty(),
            payload::Class::Values(grant) => {
                let active = owner.activate();
                let credit = match active.try_values() {
                    Ok(credit) => credit,
                    Err(error) => {
                        owner.restore(grant);
                        return Err(error);
                    }
                };
                // SAFETY: the captured owner binds this held edge and grant to the same entry.
                unsafe { active.state().values.activate(grant) };
                retain::Held {
                    bytes: None,
                    values: Some(credit),
                }
            }
            payload::Class::Bytes(grant) => {
                let active = owner.activate();
                let credit = match active.try_bytes() {
                    Ok(credit) => credit,
                    Err(error) => {
                        owner.restore(grant);
                        return Err(error);
                    }
                };
                // SAFETY: the captured owner binds this held edge and grant to the same entry.
                unsafe { active.state().bytes.activate(grant) };
                retain::Held {
                    bytes: Some(credit),
                    values: None,
                }
            }
            payload::Class::Both(values, bytes) => {
                let active = owner.activate();
                let held = (|| {
                    Ok(retain::Held {
                        values: Some(active.try_values()?),
                        bytes: Some(active.try_bytes()?),
                    })
                })();
                let held = match held {
                    Ok(held) => held,
                    Err(error) => {
                        owner.restore(values);
                        owner.restore(bytes);
                        return Err(error);
                    }
                };
                // SAFETY: both live edges and disjoint grants belong to this captured owner.
                unsafe {
                    active.state().values.activate(values);
                    active.state().bytes.activate(bytes);
                }
                held
            }
        };
        Ok(held)
    }
}

impl<A: mem::raw::Amount<3, usize>> Removal<'_, '_, '_, '_, A> {
    pub(in crate::reply) fn release(self) {
        self.owner.key.release_data(self.owner.lane, self.grant);
    }
}

impl<'arena, 'credits, 'region> Removal<'_, 'arena, 'credits, 'region, payload::Value> {
    pub(in crate::reply) fn try_retain(
        self,
    ) -> Result<retain::Held<'arena, 'credits, 'region>, rc::Error> {
        let Self { owner, grant } = self;
        let (data, grant) = payload::Value::split(grant);
        let held = match (Removal {
            owner: &mut *owner,
            grant,
        })
        .try_hold()
        {
            Ok(held) => held,
            Err(error) => {
                owner.restore(data);
                return Err(error);
            }
        };
        owner.key.release_data(owner.lane, data);
        Ok(held)
    }

    pub(in crate::reply) fn try_value(
        self,
    ) -> Result<Option<mem::Lease<LeaseEdge<'arena, 'credits, 'region>>>, rc::Error> {
        let Self { owner, grant } = self;
        let (data, grant) = payload::Value::split(grant);
        match payload::Failure::claim(grant) {
            None => {
                owner.key.release_data(owner.lane, data);
                Ok(None)
            }
            Some(grant) => owner.try_lease(data, grant).map(Some),
        }
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
        let Self { owner, grant } = self;
        let (data, grant) = payload::Value::split(grant);
        owner.try_lease(data, grant)
    }
}

const _: () = {
    assert!(size_of::<Payload<'static, 'static, 'static>>() == 7 * size_of::<usize>());
    assert!(size_of::<Owner<'static, 'static, 'static>>() == size_of::<[usize; 8]>());
};
