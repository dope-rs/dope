use core::pin;

use crate::task::storage::fixed::schedule::ready::{
    Global,
    linked::{Entry, Queue},
};

#[must_use = "a dequeued attachment must be committed"]
pub struct Dequeued<'queue, 'd, I = Global> {
    pub(in crate::task::storage::fixed::schedule) owner: pin::Pin<&'queue Queue<'d>>,
    pub(in crate::task::storage::fixed::schedule) index: I,
}

impl<'queue, 'd> Dequeued<'queue, 'd> {
    pub(in crate::task::storage::fixed::schedule::ready::linked) const fn new(
        owner: pin::Pin<&'queue Queue<'d>>,
        index: Global,
    ) -> Self {
        Self { owner, index }
    }

    pub(in crate::task::storage::fixed::schedule) fn enqueue(self, entry: pin::Pin<&Entry>) {
        self.owner.get_ref().state().enqueue(entry);
    }
}

impl<'queue, 'd, I> Dequeued<'queue, 'd, I> {
    pub(in crate::task::storage::fixed::schedule) const fn owner(
        &self,
    ) -> pin::Pin<&'queue Queue<'d>> {
        self.owner
    }
}
