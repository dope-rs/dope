use dope_core::{driver::route, io::event::receiving};
use o3::collections::{self, fixed::arena};

/// Fixed storage shared by per-connection deferred receive lanes.
pub(in crate::link::pool) struct Recvs<'d> {
    events: arena::Linked<receiving::Completion<'d>>,
}

impl<'d> Recvs<'d> {
    pub(in crate::link::pool) fn try_with_capacity(
        connection_slots: usize,
        event_slots: usize,
    ) -> Result<Self, collections::AllocationError> {
        use o3::collections::fixed::arena::Linked;
        Ok(Self {
            events: Linked::try_with_capacity(event_slots, connection_slots)?,
        })
    }

    pub(super) fn push(
        &mut self,
        index: route::SlotIndex,
        completion: receiving::Completion<'d>,
    ) -> Result<(), receiving::Completion<'d>> {
        self.events.push_back(index.raw() as usize, completion)
    }

    pub(in crate::link::pool) fn pop(
        &mut self,
        index: route::SlotIndex,
    ) -> Option<receiving::Completion<'d>> {
        self.events.pop_front(index.raw() as usize)
    }

    pub(in crate::link::pool) fn has(&self, index: route::SlotIndex) -> bool {
        !self.events.lane_is_empty(index.raw() as usize)
    }
}
