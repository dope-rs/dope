//! Complete per-entry reply-pool ownership.

use std::mem::size_of;

use o3::mem::{self, raw::Source as _, rc};

use crate::reply::{
    LaneIndex,
    arena::state::EntryKey,
    retain::{
        self,
        grant::{entries, payload},
        pool::{self, Acquire, Active, Key, LeaseEdge, Release, state},
    },
    usage,
};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Order {
    Linked,
    Detached,
}

pub(in crate::reply) struct Owner<'arena, 'credits, 'region> {
    order: Order,
    pub(in crate::reply) payload: pool::Payload<'arena, 'credits, 'region>,
}

pub(in crate::reply) struct Payload<'arena, 'credits, 'region> {
    key: Key<'arena, 'credits, 'region>,
    lane: LaneIndex,
    sum: payload::raw::Sum<'credits>,
    entry: entries::Grant<'credits, entries::Units<1, 0>>,
}

/// One removed member and its exact source owner, kept inseparable until
/// retention commits or the original acquisition is returned.
pub(in crate::reply) struct Removal<
    'a,
    'arena,
    'credits,
    'region,
    A: mem::raw::Amount<3, usize> + Into<payload::Returned>,
> {
    owner: &'a mut Payload<'arena, 'credits, 'region>,
    grant: payload::Grant<'credits, A>,
}

struct Retention<'a, 'arena, 'credits, 'region> {
    owner: &'a mut Payload<'arena, 'credits, 'region>,
    grant: payload::Grant<'credits, payload::Failure>,
}

/// A source-bound component transfer produced only by the captured removal.
struct Transfer<'arena, 'credits, 'region, A: mem::raw::Sum<3, usize>> {
    active: Active<'arena, 'credits, 'region>,
    grant: payload::Grant<'credits, A>,
}

trait Component: mem::raw::Sum<3, usize> {
    fn axis<'a, 'credits>(
        entry: &'a state::Entry<'credits>,
    ) -> &'a state::raw::Axis<'credits, Self>;
}

impl Component for payload::Values {
    fn axis<'a, 'credits>(
        entry: &'a state::Entry<'credits>,
    ) -> &'a state::raw::Axis<'credits, Self> {
        &entry.values
    }
}

impl Component for payload::Bytes {
    fn axis<'a, 'credits>(
        entry: &'a state::Entry<'credits>,
    ) -> &'a state::raw::Axis<'credits, Self> {
        &entry.bytes
    }
}

impl<A: Component> Transfer<'_, '_, '_, A> {
    fn commit(self) {
        // SAFETY: Retention created both this target and component from its exact
        // captured owner. The live edge prevents entry reuse during the transfer.
        unsafe { A::axis(self.active.state()).activate(self.grant) };
    }
}

impl<'arena, 'credits, 'region> Owner<'arena, 'credits, 'region> {
    pub(in crate::reply::retain::pool) fn new(
        admitted: pool::Registration<'arena, 'credits, 'region, entries::Units<1, 0>>,
        key: EntryKey,
    ) -> Self {
        let (lane, entry) = admitted.into_parts();
        let pool = lane.pool();
        let index = lane.index();
        let sum = payload::raw::Sum::new(pool.lanes().credits.payload(index));
        let key = pool.bind(key);
        Self {
            order: Order::Linked,
            payload: Payload {
                key,
                lane: index,
                sum,
                entry,
            },
        }
    }

    pub(in crate::reply) fn clear_order(&mut self) {
        self.order = Order::Detached;
    }

    pub(in crate::reply) fn retire(&self) {
        self.payload.key.retire();
    }

    pub(in crate::reply) fn cancel_entry(&self) {
        self.payload.key.cancel_entry(self.payload.lane);
    }

    pub(in crate::reply) fn is_empty(&self) -> bool {
        matches!(self.order, Order::Detached)
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
        A: mem::raw::Amount<3, usize> + Into<payload::Returned> + Into<payload::Requested>,
    >(
        &mut self,
        key: EntryKey,
        amount: A,
    ) -> Result<(), Acquire> {
        self.key.try_acquire(self.lane, key, &mut self.sum, amount)
    }

    fn restore<A: mem::raw::Amount<3, usize>>(&mut self, grant: payload::Grant<'credits, A>) {
        // SAFETY: only Removal's rollback returns pieces of its captured source.
        unsafe { self.sum.merge(grant) };
    }

    pub(in crate::reply) fn try_batch(
        &mut self,
    ) -> Result<retain::Batch<'arena, 'credits, 'region>, rc::Error> {
        Ok(retain::Batch {
            edge: LeaseEdge::try_new(self.activate())?,
            sum: self.sum.drain(),
        })
    }

    pub(in crate::reply) fn len(&self) -> usize {
        self.sum.len()
    }

    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        self.sum.usage()
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
        A: mem::raw::Amount<3, usize> + Into<payload::Returned>,
    {
        // SAFETY: the owning slot/removal proves membership in this exact source;
        // take splits it under the exclusive borrow, then Removal retains it.
        let grant = unsafe { self.sum.take(amount) };
        Removal { owner: self, grant }
    }
}

impl<'arena, 'credits, 'region> Retention<'_, 'arena, 'credits, 'region> {
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
                Transfer { active, grant }.commit();
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
                Transfer { active, grant }.commit();
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
                Transfer {
                    active,
                    grant: values,
                }
                .commit();
                Transfer {
                    active,
                    grant: bytes,
                }
                .commit();
                held
            }
        };
        Ok(held)
    }
}

impl<A: mem::raw::Amount<3, usize> + Into<payload::Returned>> Removal<'_, '_, '_, '_, A> {
    pub(in crate::reply) fn release(self) {
        self.owner.key.release_data(self.grant);
    }
}

impl<'arena, 'credits, 'region> Removal<'_, 'arena, 'credits, 'region, payload::Value> {
    pub(in crate::reply) fn try_retain(
        self,
    ) -> Result<retain::Held<'arena, 'credits, 'region>, rc::Error> {
        let Self { owner, grant } = self;
        let (data, grant) = payload::Value::split(grant);
        let held = match (Retention {
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
        owner.key.release_data(data);
        Ok(held)
    }

    pub(in crate::reply) fn try_value(
        self,
    ) -> Result<Option<mem::Lease<LeaseEdge<'arena, 'credits, 'region>>>, rc::Error> {
        let Self { owner, grant } = self;
        let (data, grant) = payload::Value::split(grant);
        match payload::Failure::claim(grant) {
            None => {
                owner.key.release_data(data);
                Ok(None)
            }
            Some(grant) => {
                let edge = match LeaseEdge::try_new(owner.activate()) {
                    Ok(edge) => edge,
                    Err(error) => {
                        owner.restore(data);
                        owner.restore(grant);
                        return Err(error);
                    }
                };
                owner.key.release_data(data);
                Ok(Some(mem::Lease::new(edge, grant)))
            }
        }
    }
}

const _: () = {
    assert!(size_of::<Payload<'static, 'static, 'static>>() == 7 * size_of::<usize>());
    assert!(size_of::<Owner<'static, 'static, 'static>>() == size_of::<[usize; 8]>());
};
