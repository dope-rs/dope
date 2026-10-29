use core::{marker, pin};

use o3::base::marker::ThreadBound;

use crate::{
    core::driver::schedule::ready::task::{self, topology},
    task::storage::fixed::schedule::ready::{
        Global,
        linked::{Drain, Entry, Prepared, State},
    },
};

#[pin_project::pin_project(!Unpin)]
pub(in crate::task::storage::fixed::schedule) struct Queue<'d> {
    state: State,
    #[pin]
    slot: topology::Slot<'d, Global>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<'d> Queue<'d> {
    pub(in crate::task::storage::fixed::schedule) fn new() -> Self {
        Self {
            state: State::new(),
            slot: topology::Slot::new(),
            thread: marker::PhantomData,
        }
    }

    pub(in crate::task::storage::fixed::schedule) fn try_prepare<'a>(
        self: pin::Pin<&'a Self>,
        index: Global,
        parent: task::Parent<'d>,
    ) -> Result<(topology::Edge<'a, 'd, Global>, Prepared<'a, 'd>), task::Error> {
        let edge = self.get_ref().slot.try_attach(parent, index)?;
        Ok((edge, Prepared::new(self)))
    }

    pub(in crate::task::storage::fixed::schedule) fn detach(
        self: pin::Pin<&Self>,
        entry: pin::Pin<&Entry>,
    ) {
        self.state.store.detach(entry);
    }

    /// The plane retains the exact resident; a duplicate wake preserves its FIFO position.
    #[inline]
    pub(in crate::task::storage::fixed::schedule) fn merge(
        self: pin::Pin<&Self>,
        entry: pin::Pin<&Entry>,
    ) {
        if !entry.is_queued() {
            self.state.enqueue(entry);
        }
    }

    pub(in crate::task::storage::fixed::schedule) fn snapshot(
        self: pin::Pin<&Self>,
    ) -> Option<Drain<'_, 'd>> {
        if self.state.is_empty() && self.slot.is_empty() {
            return None;
        }
        Some(Drain::new(self, self.state.start()))
    }

    pub(in crate::task::storage::fixed::schedule) const fn state(&self) -> &State {
        &self.state
    }

    pub(in crate::task::storage::fixed::schedule) const fn slot(
        &self,
    ) -> &topology::Slot<'d, Global> {
        &self.slot
    }
}
