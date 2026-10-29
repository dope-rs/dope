use std::pin::Pin;

use crate::task::storage::fixed::schedule::ready::{
    Global,
    owned::{Dequeued, Queue, Resident},
};

/// # Safety
/// The exact index must retain a live admission to this queue. Its exclusive
/// owner must exclude polling, detachment, and recycling until this transition.
pub(in crate::task::storage) unsafe fn detach<Source>(
    queue: Pin<&Queue<'_, Source>>,
    index: Global,
) {
    let dequeued = match queue.resident(index) {
        Resident::Linked(entry) => {
            let store = &queue.store;
            store
                .owner(entry.lane)
                .remove(store, entry.lane, entry.index);
            Dequeued(entry)
        }
        Resident::Idle(dequeued) => dequeued,
    };
    dequeued.detach();
}
