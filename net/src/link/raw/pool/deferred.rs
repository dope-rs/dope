use dope_core::driver::token::{SlotIndex, Token};
use dope_core::io::RecvEvent;
use o3::collections::LinkedArena;

pub(super) struct DeferredRecv<'d> {
    pub(super) token: Token,
    pub(super) more: bool,
    pub(super) event: RecvEvent<'d>,
}

/// Fixed storage shared by per-connection deferred receive lanes.
pub(super) struct DeferredRecvs<'d> {
    events: LinkedArena<DeferredRecv<'d>>,
}

impl<'d> DeferredRecvs<'d> {
    pub(super) fn with_capacity(connection_slots: usize, event_slots: usize) -> Self {
        Self {
            events: LinkedArena::with_capacity(event_slots, connection_slots),
        }
    }

    pub(super) fn push(
        &mut self,
        index: SlotIndex,
        token: Token,
        more: bool,
        event: RecvEvent<'d>,
    ) -> Result<(), DeferredRecv<'d>> {
        self.events
            .push_back(index.raw() as usize, DeferredRecv { token, more, event })
    }

    pub(super) fn pop(&mut self, index: SlotIndex) -> Option<DeferredRecv<'d>> {
        self.events.pop_front(index.raw() as usize)
    }

    pub(super) fn clear(&mut self, index: SlotIndex) {
        while self.pop(index).is_some() {}
    }
}
