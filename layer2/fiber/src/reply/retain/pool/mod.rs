//! Reply resource ownership, fair lanes, and retained entry slots.

use std::{mem::size_of, num::NonZero};

use o3::{
    alloc::{self, tail},
    depot,
    depot::index::Initialized,
    mem,
    mem::{Lease, Return, credit::fair, rc},
};

use crate::reply::{
    LaneIndex, Length,
    arena::state::EntryKey,
    layout, retain,
    retain::{
        grant::payload,
        pool::{lanes::Lanes, returns::Queue, state::Entry},
    },
    usage,
};

mod lane;
mod lanes;
mod owner;
pub(in crate::reply) mod raw;
mod returns;
mod state;

pub(in crate::reply) use lane::{Lane, Release};
pub(in crate::reply) use lanes::{Registration, Slots};
pub(in crate::reply) use owner::{Owner, Payload, Removal};

pub(in crate::reply) enum Acquire {
    Capacity(usage::Capacity),
    Backpressure(usage::Backpressure),
}

impl Acquire {
    fn new(cause: fair::Cause<usize, 3>) -> Self {
        match cause {
            fair::Cause::Overflow {
                axis,
                held,
                requested: added,
            } => Self::Capacity(usage::Capacity::Arithmetic {
                resource: Self::resource(axis),
                current: usage::Current::new(held),
                added: usage::Added::new(added),
            }),
            fair::Cause::Exhausted {
                axis,
                required,
                available: free,
            } => Self::Backpressure(usage::Backpressure::new(
                Self::resource(axis),
                usage::Required::new(required),
                usage::Available::new(free),
            )),
        }
    }

    fn resource(axis: fair::Axis<3>) -> usage::Resource {
        if axis == fair::Axis::at::<0>() {
            usage::Resource::Items
        } else if axis == fair::Axis::at::<1>() {
            usage::Resource::Values
        } else {
            usage::Resource::Bytes
        }
    }
}

/// Complete owner of reply credits, wake topology, and retained entry state.
pub(crate) struct Pool<'credits, 'region> {
    storage: tail::Block<Core<'credits, 'region>, Entry<'credits>, NonZero<u32>>,
}

impl<'credits, 'region> Pool<'credits, 'region> {
    pub(in crate::reply) fn try_new(
        layout: layout::Layout,
        slots: &'credits Slots<'region>,
    ) -> Result<Self, alloc::Error> {
        let plan = tail::Plan::try_new(layout.entries())?;
        let storage = plan.try_allocate()?.fill(
            Core {
                lanes: Lanes::new(slots),
                released: Queue::new(),
            },
            |_, index| {
                // SAFETY: the exact initialized tail index belongs to this arena layout.
                Entry::new(unsafe { depot::raw::Compact::from_raw(index) })
            },
        );
        Ok(Self { storage })
    }

    pub(in crate::reply) fn shared(&self) -> usize {
        self.lanes().credits.shared_slots()[0]
    }

    pub(in crate::reply) fn used(&self) -> Length {
        self.lanes().credits.used_entries()
    }

    pub(in crate::reply) fn has_pending(&self) -> bool {
        self.lanes().has_pending()
    }

    pub(in crate::reply) fn advance(&self) {
        self.lanes().advance();
    }

    fn lanes(&self) -> &Lanes<'credits, 'region> {
        &tail::View::new(&self.storage).header().lanes
    }

    fn bind<'arena>(&'arena self, entry: EntryKey) -> Key<'arena, 'credits, 'region> {
        // SAFETY: the arena and this tail share the exact validated entry count;
        // Resident derives from the allocation pointer and retains its provenance.
        let resident = unsafe {
            alloc::raw::Resident::new(tail::View::new(&self.storage), entry.index().get() as u32)
        };
        let key = Key { resident };
        key.state().life.bind(entry.generation());
        key
    }

    pub(in crate::reply) fn pop_released(&self) -> Option<Released<'_, 'credits>> {
        let slot = tail::View::new(&self.storage)
            .header()
            .released
            .pop(tail::View::new(&self.storage).as_slice())?;
        Some(Released { slot })
    }

    pub(in crate::reply) fn has_released(&self) -> bool {
        !tail::View::new(&self.storage).header().released.is_empty()
    }
}

struct Core<'credits, 'region> {
    lanes: Lanes<'credits, 'region>,
    released: Queue,
}

struct Parts<'arena, 'credits, 'region> {
    core: &'arena Core<'credits, 'region>,
    active: Active<'arena, 'credits, 'region>,
    states: &'arena [Entry<'credits>],
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::reply::retain::pool) struct Key<'arena, 'credits, 'region> {
    resident: alloc::raw::Resident<'arena, Core<'credits, 'region>, Entry<'credits>, NonZero<u32>>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::reply::retain) struct Active<'arena, 'credits, 'region> {
    key: Key<'arena, 'credits, 'region>,
}

pub(in crate::reply) struct Released<'arena, 'credits> {
    slot: &'arena Entry<'credits>,
}

pub(in crate::reply) struct LeaseEdge<'arena, 'credits, 'region> {
    active: Active<'arena, 'credits, 'region>,
}

impl<'arena, 'credits, 'region> Key<'arena, 'credits, 'region> {
    fn state(self) -> &'arena Entry<'credits> {
        self.resident.get()
    }

    fn entry(self) -> EntryKey {
        let slot = self.state();
        EntryKey::new(slot.index(), slot.life.generation())
    }

    fn activate(self, lane: LaneIndex) -> Active<'arena, 'credits, 'region> {
        self.state().life.activate(lane);
        Active { key: self }
    }

    fn retire(self) {
        if !self.state().life.is_vacant() {
            Active { key: self }.retire();
        }
    }

    fn cancel_entry(self, lane: LaneIndex) {
        let key = self.entry();
        self.parts().core.lanes.cancel_entry(lane, key);
    }

    fn try_acquire<A>(
        self,
        lane: LaneIndex,
        key: EntryKey,
        sum: &mut payload::raw::Sum<'credits>,
        amount: A,
    ) -> Result<(), Acquire>
    where
        A: mem::raw::Amount<3, usize> + Into<payload::Returned> + Into<payload::Requested>,
    {
        self.parts().core.lanes.take_waiting(lane, key, sum, amount)
    }

    fn release_data<A>(self, grant: payload::Grant<'credits, A>)
    where
        A: mem::raw::Amount<3, usize> + Into<payload::Returned>,
    {
        self.parts().core.lanes.release_data(grant);
    }

    fn parts(self) -> Parts<'arena, 'credits, 'region> {
        // SAFETY: Entry's immutable ordinal is exactly the one used to bind
        // this resident; the original allocation provenance and lifetime survive.
        let view = unsafe { self.resident.view(self.state().index().get() as u32) };
        let states = view.as_slice();
        let core = view.header();
        Parts {
            core,
            active: Active { key: self },
            states,
        }
    }
}

impl<'arena, 'credits, 'region> Active<'arena, 'credits, 'region> {
    fn state(self) -> &'arena Entry<'credits> {
        self.key.state()
    }

    fn try_bytes(self) -> Result<retain::Bytes<'arena, 'credits, 'region>, rc::Error> {
        self.state().bytes.try_hold()?;
        Ok(retain::Bytes::new(self))
    }

    pub(in crate::reply::retain) fn try_share(self) -> Result<Self, rc::Error> {
        self.state().bytes.try_retain()?;
        Ok(self)
    }

    pub(in crate::reply::retain) fn drop_bytes(self) {
        let Some(grant) = self.state().bytes.release() else {
            return;
        };
        let parts = self.parts();
        parts.core.lanes.release_data(grant);
        parts.finish();
    }

    fn try_values(self) -> Result<retain::Values<'arena, 'credits, 'region>, rc::Error> {
        self.state().values.try_hold()?;
        Ok(retain::Values::new(self))
    }

    pub(in crate::reply::retain) fn drop_values(self) {
        let Some(grant) = self.state().values.release() else {
            return;
        };
        let parts = self.parts();
        parts.core.lanes.release_data(grant);
        parts.finish();
    }

    fn lane(self) -> LaneIndex {
        self.state().life.lane()
    }

    fn retire(self) {
        let state = self.state();
        let retained = state.is_active();
        state.life.retire(retained);
    }

    fn drop_lease(self) {
        if self.state().drop_lease() == rc::Release::Last {
            self.parts().finish();
        }
    }

    pub(in crate::reply::retain) fn wake(self) {
        let parts = self.parts();
        parts.core.lanes.wake(parts.active.lane());
    }

    fn parts(self) -> Parts<'arena, 'credits, 'region> {
        self.key.parts()
    }
}

impl<'arena, 'credits, 'region> LeaseEdge<'arena, 'credits, 'region> {
    fn try_new(active: Active<'arena, 'credits, 'region>) -> Result<Self, rc::Error> {
        active.state().try_lease()?;
        Ok(Self { active })
    }

    pub(in crate::reply::retain) fn returned(&self, value: payload::Value) {
        self.active
            .parts()
            .core
            .lanes
            .returned(payload::Returned::Value(value));
    }

    pub(in crate::reply::retain) fn wake(&self) {
        self.active.wake();
    }
}

impl Released<'_, '_> {
    pub(in crate::reply) fn settle(self) -> depot::raw::Proof<(Self, EntryKey), Initialized<Self>> {
        let key = EntryKey::new(self.slot.index(), self.slot.life.settle());
        // SAFETY: the dequeued release owns this generation's final retention
        // settlement; its arena entry remains occupied until that settlement is consumed.
        unsafe { depot::raw::Proof::new((self, key)) }
    }
}

impl<'credits> Return for LeaseEdge<'_, 'credits, '_> {
    type Item = payload::Grant<'credits, payload::Used>;

    fn return_item(self, grant: Self::Item) {
        let parts = self.active.parts();
        parts.core.lanes.refund(grant);
    }
}

impl Drop for LeaseEdge<'_, '_, '_> {
    fn drop(&mut self) {
        self.active.drop_lease();
    }
}

impl Parts<'_, '_, '_> {
    fn finish(&self) {
        let state = self.active.state();
        if state.is_active() {
            return;
        }
        let Some(ready) = returns::Ready::new(state) else {
            return;
        };
        let lane = self.core.released.push(self.states, ready);
        self.core.lanes.wake(lane);
    }
}

const _: () = {
    assert!(size_of::<Pool<'static, 'static>>() == size_of::<usize>());
    assert!(size_of::<Key<'static, 'static, 'static>>() == size_of::<usize>());
    assert!(size_of::<Option<Key<'static, 'static, 'static>>>() == size_of::<usize>());
    assert!(size_of::<Active<'static, 'static, 'static>>() == size_of::<usize>());
    assert!(size_of::<Released<'static, 'static>>() == size_of::<usize>());
    assert!(size_of::<LeaseEdge<'static, 'static, 'static>>() == size_of::<usize>());
    assert!(size_of::<Lease<LeaseEdge<'static, 'static, 'static>>>() == 4 * size_of::<usize>());
};
