//! Incremental reply-entry retirement.

use std::mem;

use o3::{
    depot,
    depot::{arena::slab, index::Initialized},
};

use crate::reply::{
    self, LaneIndex,
    arena::{retire::Retired, state},
    retain::pool::{self, Release},
    usage::Footprint,
};

pub(in crate::reply::arena) enum Advance {
    Draining(Retired),
    Complete(Option<LaneIndex>),
}

impl<'arena, 'credits, 'region, D: Footprint, E, K>
    state::Book<'arena, 'credits, 'region, D, E, K>
{
    /// Retires each occupied attachment once in its original lane.
    pub(in crate::reply::arena) fn retire(&mut self, retired: Retired) -> Advance {
        let index = retired.index();
        // SAFETY: the original arena joins its affine retirement with the
        // entry that remains occupied until this retirement and retention finish.
        let live = unsafe {
            depot::raw::Proof::<_, Initialized<Retired>>::new((
                (&mut self.slots.0, retired),
                index.into(),
            ))
        };
        let (mut occupied, retired) = slab::Occupied::from_live(live);
        let entry = occupied.get_mut();
        let state = match &mut entry.body {
            state::EntryBody::Drained => {
                entry.owners.cancel_entry();
                let (lane, release) = occupied.remove().owners.unbind();
                self.release(lane, release);
                return Advance::Complete(Some(lane));
            }
            state::EntryBody::Draining(state) => state,
            body @ state::EntryBody::Attached(_) => {
                let slot = match mem::replace(body, state::EntryBody::Drained) {
                    state::EntryBody::Attached(state::Attached(slot)) => slot,
                    _ => unreachable!("attached"),
                };
                match mem::replace(body, state::EntryBody::Draining(slot)) {
                    state::EntryBody::Drained => {}
                    _ => unreachable!("drained"),
                }
                let state::EntryBody::Draining(state) = body else {
                    unreachable!("draining")
                };
                entry.owners.cancel_entry();
                let lane = entry.owners.lane();
                let route = self.order.route(lane).into_state();
                route.live = route.live.wrapping_sub(1);
                state
            }
        };
        // SAFETY: both fields remain exclusively borrowed from this
        // original occupied entry throughout its retirement step.
        if let reply::Drain::Dropped =
            unsafe { reply::raw::Retire::new(state, &mut entry.owners) }.advance()
        {
            return Advance::Draining(retired);
        }
        entry.body = state::EntryBody::Drained;
        entry.owners.retire();
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
            // SAFETY: settlement retains the original occupied entry
            // while the arena loans the slab that owns it.
            let live = unsafe {
                released
                    .settle()
                    .map(|(released, index)| ((&mut self.slots.0, released), index.into()))
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
        if matches!(release, Release::Reserved) && self.order.contains(lane) {
            self.order.reserve(lane);
        }
    }
}
