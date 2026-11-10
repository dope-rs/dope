//! Incremental reply-entry retirement.

use std::{marker::PhantomData, mem};

use o3::{
    depot,
    depot::{arena::slab, index::Initialized},
};

use crate::reply::{
    self, LaneIndex,
    arena::{retire::Retired, state},
    retain::pool::{self, Release},
    slot::End,
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
        match &mut entry.state.end {
            End::Drained => {
                entry.owners.cancel_entry();
                let (lane, release) = occupied.remove().owners.unbind();
                self.release(lane, release);
                return Advance::Complete(Some(lane));
            }
            End::Draining | End::DrainingFailed(_) => {}
            end => {
                if let End::Failed(failure) = mem::replace(end, End::Draining) {
                    *end = End::DrainingFailed(failure);
                }
                entry.owners.cancel_entry();
            }
        }
        let mut slot = reply::Slot {
            state: &mut entry.state,
            owners: &mut entry.owners,
            owner: PhantomData,
        };
        if slot.drop_next() {
            return Advance::Draining(retired);
        }
        entry.state.end = End::Drained;
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
            if !matches!(entry.state.end, End::Drained) || !entry.owners.is_empty() {
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
