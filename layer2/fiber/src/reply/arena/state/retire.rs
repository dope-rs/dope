//! Incremental reply-entry retirement.

use std::mem;

use o3::{
    depot,
    depot::{arena::slab, index::Initialized},
};

use crate::reply::{
    self, LaneIndex,
    arena::{
        retire::{Active, Retired},
        state,
    },
    retain::pool::{self, Release},
};

pub(in crate::reply::arena) enum Start {
    Drained(LaneIndex),
    Draining(Active),
}

pub(in crate::reply::arena) enum Advance {
    Draining(Active),
    Complete(Option<LaneIndex>),
}

pub(in crate::reply::arena) struct Retire<'a, 'arena, 'credits, 'region, D, E, K> {
    state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
}

impl<'a, 'arena, 'credits, 'region, D, E, K> Retire<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply::arena) const fn new(
        state: &'a mut state::Book<'arena, 'credits, 'region, D, E, K>,
    ) -> Self {
        Self { state }
    }

    /// Retires each occupied attachment once in its original lane.
    pub(in crate::reply::arena) fn begin(&mut self, retired: Retired) -> Start {
        let index = retired.key().index();
        // SAFETY: the original arena joins its affine retirement with the
        // entry that remains occupied until this retirement and retention finish.
        let live = unsafe {
            depot::raw::Proof::<_, Initialized<Retired>>::new((
                (&mut self.state.slots.0, retired),
                index.into(),
            ))
        };
        let (mut occupied, retired) = slab::Occupied::from_live(live);
        let entry = occupied.get_mut();
        entry.owners.cancel_entry();
        match mem::replace(&mut entry.body, state::EntryBody::Drained) {
            state::EntryBody::Drained => {
                let (lane, release) = occupied.remove().owners.unbind();
                self.release(lane, release);
                Start::Drained(lane)
            }
            state::EntryBody::Draining(slot) => {
                entry.body = state::EntryBody::Draining(slot);
                Start::Draining(Active::new(retired))
            }
            state::EntryBody::Attached(state::Attached(slot)) => {
                let lane = entry.owners.lane();
                let route = self.state.order.route(lane).into_state();
                route.live = route.live.wrapping_sub(1);
                entry.body = state::EntryBody::Draining(slot);
                Start::Draining(Active::new(retired))
            }
        }
    }

    pub(in crate::reply::arena) fn advance(&mut self, active: Active) -> Advance {
        let index = active.retired().key().index();
        // SAFETY: active retirement retains this entry until draining completes.
        let live = unsafe {
            depot::raw::Proof::<_, Initialized<Active>>::new((
                (&mut self.state.slots.0, active),
                index.into(),
            ))
        };
        let (mut occupied, active) = slab::Occupied::from_live(live);
        let entry = occupied.get_mut();
        match &mut entry.body {
            // SAFETY: both fields remain exclusively borrowed from this
            // original occupied entry throughout its retirement step.
            state::EntryBody::Draining(state) => {
                match unsafe { reply::raw::Retire::new(state, &mut entry.owners) }.advance() {
                    reply::Drain::Dropped => return Advance::Draining(active),
                    reply::Drain::Empty => {
                        entry.body = state::EntryBody::Drained;
                        entry.owners.retire();
                    }
                }
            }
            state::EntryBody::Attached(_) => return Advance::Complete(None),
            state::EntryBody::Drained => {}
        }
        if !entry.owners.is_empty() {
            return Advance::Complete(None);
        }
        let (lane, release) = occupied.remove().owners.unbind();
        self.release(lane, release);
        Advance::Complete(Some(lane))
    }

    pub(in crate::reply::arena) fn retained(
        &mut self,
        released: pool::Released<'_, 'credits>,
    ) -> Option<LaneIndex> {
        let (lane, release) = {
            // SAFETY: settlement retains the original released entry and its exact
            // generation while the arena loans the slab that owns that entry.
            let live = unsafe {
                released.settle().map(|(released, key)| {
                    ((&mut self.state.slots.0, released), key.index().into())
                })
            };
            let (occupied, _held) = slab::Occupied::from_live(live);
            let entry = occupied.get();
            if !matches!(entry.body, state::EntryBody::Drained) || !entry.owners.is_empty() {
                return None;
            }
            occupied.remove().owners.unbind()
        };
        self.release(lane, release);
        Some(lane)
    }

    fn release(&mut self, lane: LaneIndex, release: Release) {
        if matches!(release, Release::Reserved) && self.state.order.contains(lane) {
            self.state.order.reserve(lane);
        }
    }
}
