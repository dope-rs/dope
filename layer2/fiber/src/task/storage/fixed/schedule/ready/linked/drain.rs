use core::{mem, pin};

use crate::{
    core::driver::schedule::app,
    task::storage::fixed::schedule::ready::{
        Global,
        linked::{Dequeued, Queue, Side, State},
    },
};

pub(in crate::task::storage::fixed::schedule) struct Drain<'a, 'd> {
    queue: pin::Pin<&'a Queue<'d>>,
    side: Option<Side>,
}

impl<'a, 'd> Drain<'a, 'd> {
    pub(super) const fn new(queue: pin::Pin<&'a Queue<'d>>, side: Option<Side>) -> Self {
        Self { queue, side }
    }

    pub(in crate::task::storage::fixed::schedule) fn peek(&self) -> Option<Global> {
        self.side
            .and_then(|side| self.state().store.peek(side))
            .or_else(|| self.queue.get_ref().slot().peek())
    }

    pub(in crate::task::storage::fixed::schedule) fn pause(self) {
        let this = mem::ManuallyDrop::new(self);
        if let Some(side) = this.side {
            this.state().pause(side);
        }
    }

    fn state(&self) -> &State {
        self.queue.get_ref().state()
    }
}

impl<'a, 'd> app::Drain for Drain<'a, 'd> {
    type Candidate<'b>
        = Global
    where
        Self: 'b;
    type Item<'b>
        = Dequeued<'a, 'd>
    where
        Self: 'b;

    fn next_when<P>(
        &mut self,
        take: impl FnOnce() -> Option<P>,
    ) -> app::Selection<Self::Item<'_>, P, Self::Candidate<'_>> {
        if let Some(side) = self.side
            && let Some(front) = self.state().store.front(side)
        {
            let Some(permit) = take() else {
                return Some(Err(front.index()));
            };
            let index = front.remove();
            return Some(Ok((Dequeued::new(self.queue, index), permit)));
        }
        let index = self.queue.get_ref().slot().peek()?;
        let Some(permit) = take() else {
            return Some(Err(index));
        };
        let index = self.queue.get_ref().slot().pop()?;
        Some(Ok((Dequeued::new(self.queue, index), permit)))
    }
}

impl Drop for Drain<'_, '_> {
    fn drop(&mut self) {
        if let Some(side) = self.side {
            self.state().seal(side);
        }
    }
}
