use core::pin;

use crate::{
    abi,
    task::storage::set::slot::{self, state::State},
};

pub(in crate::task::storage::set) struct Queued<'a, 'd, F: abi::Fiber<'d>, M, D, G = ()> {
    slot: pin::Pin<&'a slot::Slot<'d, F, M, D, G>>,
}

impl<'a, 'd, F: abi::Fiber<'d>, M, D, G> Queued<'a, 'd, F, M, D, G> {
    /// # Safety
    /// Retain the slot's runnable membership and exclusive storage token.
    pub(in crate::task::storage::set) const unsafe fn new(
        slot: pin::Pin<&'a slot::Slot<'d, F, M, D, G>>,
    ) -> Self {
        Self { slot }
    }

    pub(in crate::task::storage::set) fn wake(self) {
        self.slot.project_ref().value.root().parent().wake();
    }

    pub(in crate::task::storage::set) fn start(self) -> slot::Running<'a, 'd, F, M, D, G> {
        self.slot.value.transition(State::Running);
        slot::Running(self.slot)
    }
}
